"""Bounded client for the Codex desktop app's owner-routed IPC socket.

INFRA-223 (2026-09-03): the Codex desktop app owns every visible Sol
thread. It serves a private IPC router on ``~/.codex/ipc/ipc.sock``
(4-byte little-endian length-prefixed UTF-8 JSON frames). A client
registers with an ``initialize`` request and is issued a ``clientId``;
subsequent ``request`` frames are routed by the app to the one client
that ``canHandle`` them -- for thread-scoped methods, the window that
OWNS the thread -- so a turn started this way belongs to the desktop
app and survives this client's disconnect. This module implements only
the three operations Hermes needs: ``initialize``,
``thread-owner-discovery`` and the owner-routed
``thread-follower-start-turn``; nothing here spawns an App Server.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import struct
import uuid
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_DESKTOP_IPC_SOCKET = Path.home() / ".codex" / "ipc" / "ipc.sock"
LOCAL_HOST_ID = "local"

#: Per-method request versions the desktop validates (``request-version-
#: mismatch`` otherwise); copied from the app's own method/version table.
REQUEST_VERSIONS = {
    "thread-owner-discovery": 1,
    "thread-follower-start-turn": 2,
}

#: Broadcast versions (the desktop drops mismatched versions silently).
BROADCAST_VERSIONS = {
    "thread-stream-following-changed": 1,
    "thread-stream-following-status-requested": 1,
    "thread-stream-state-changed": 11,
}

_MAX_FRAME = 256 * 1024 * 1024


class DesktopIpcUnavailable(RuntimeError):
    """The desktop IPC socket is absent, refused, or answered with an error."""


@dataclass(frozen=True, slots=True)
class IpcResponse:
    method: str | None
    result_type: str
    result: Any
    error: str | None
    handled_by_client_id: str | None


def desktop_ipc_socket(socket_path: str | None = None) -> Path | None:
    """The desktop IPC socket when it is present on this host, else None."""

    endpoint = (
        Path(socket_path) if socket_path is not None else DEFAULT_DESKTOP_IPC_SOCKET
    )
    return endpoint if endpoint.is_socket() else None


class DesktopIpcClient:
    """One bounded connection: connect, initialize, act, close."""

    def __init__(
        self,
        socket_path: Path,
        *,
        client_type: str = "hermes-orchestrator",
        timeout: float = 15.0,
    ) -> None:
        self._socket_path = socket_path
        self._client_type = client_type
        self._timeout = timeout
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self.client_id: str | None = None

    async def __aenter__(self) -> DesktopIpcClient:
        await self.connect()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def connect(self) -> str:
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_unix_connection(str(self._socket_path)), self._timeout
            )
        except (TimeoutError, OSError) as error:
            raise DesktopIpcUnavailable(
                f"desktop IPC socket {self._socket_path} is unavailable: {error}"
            ) from error
        response = await self._request(
            "initialize", {"clientType": self._client_type}, version=None
        )
        client_id = (
            response.result.get("clientId")
            if isinstance(response.result, dict)
            else None
        )
        if response.result_type != "success" or not isinstance(client_id, str):
            raise DesktopIpcUnavailable("desktop IPC initialize was not accepted")
        self.client_id = client_id
        return client_id

    async def close(self) -> None:
        writer, self._writer = self._writer, None
        self._reader = None
        if writer is not None:
            writer.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(writer.wait_closed(), 2.0)

    async def discover_owner(
        self, conversation_id: str, *, host_id: str = LOCAL_HOST_ID
    ) -> str | None:
        """The clientId of the window owning ``conversation_id``, or None."""

        response = await self._request(
            "thread-owner-discovery",
            {"hostId": host_id, "conversationId": conversation_id},
        )
        if response.result_type == "error":
            if response.error == "no-client-found":
                return None
            raise DesktopIpcUnavailable(
                f"thread-owner-discovery failed: {response.error}"
            )
        return response.handled_by_client_id

    async def start_turn(
        self,
        conversation_id: str,
        *,
        owner_client_id: str,
        turn_request: dict[str, Any],
        context: dict[str, Any] | None = None,
    ) -> Any:
        """Ask the OWNER window to start a turn on its own thread."""

        response = await self._request(
            "thread-follower-start-turn",
            {
                "conversationId": conversation_id,
                "turnStart": {"request": turn_request, "context": context or {}},
            },
            target_client_id=owner_client_id,
        )
        if response.result_type != "success":
            raise DesktopIpcUnavailable(
                f"thread-follower-start-turn failed: {response.error}"
            )
        return response.result

    async def request_stream_snapshot(
        self,
        conversation_id: str,
        *,
        owner_client_id: str,
        host_id: str = LOCAL_HOST_ID,
        timeout: float | None = None,
    ) -> dict[str, Any] | None:
        """Ask the OWNER for its live conversation state snapshot.

        Mirrors the desktop's own follower handshake: announce following,
        request the following status, and read the owner's targeted
        ``thread-stream-state-changed`` snapshot broadcast (patches are
        skipped until a snapshot arrives). Returns ``conversationState``
        or ``None`` when no snapshot arrives in time -- callers fail
        closed on ``None``. Following is withdrawn before returning.
        """

        wait = self._timeout if timeout is None else timeout
        params = {"conversationId": conversation_id, "hostId": host_id}
        await self._broadcast(
            "thread-stream-following-changed",
            {**params, "following": True},
            target_client_ids=[owner_client_id],
        )
        try:
            await self._broadcast("thread-stream-following-status-requested", params)
            deadline = asyncio.get_running_loop().time() + wait
            while True:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    return None
                try:
                    frame = await asyncio.wait_for(self._read_frame(), remaining)
                except (TimeoutError, DesktopIpcUnavailable):
                    return None
                if not self._is_owner_snapshot_for_me(
                    frame,
                    conversation_id=conversation_id,
                    owner_client_id=owner_client_id,
                    host_id=host_id,
                ):
                    continue
                change = (frame.get("params") or {}).get("change") or {}
                state = change.get("conversationState")
                return state if isinstance(state, dict) else None
        finally:
            with suppress(Exception):
                await self._broadcast(
                    "thread-stream-following-changed",
                    {**params, "following": False},
                    target_client_ids=[owner_client_id],
                )

    def _is_owner_snapshot_for_me(
        self,
        frame: dict[str, Any],
        *,
        conversation_id: str,
        owner_client_id: str,
        host_id: str,
    ) -> bool:
        """Sol correction 98ea8970: only the discovered OWNER's snapshot,
        addressed to this initialized client, for this conversation and
        host, is authoritative. Anything else -- a non-owner or stale
        client's state, an untargeted broadcast, another host, patches --
        is ignored until the bounded timeout."""

        if frame.get("type") != "broadcast":
            return False
        if frame.get("method") != "thread-stream-state-changed":
            return False
        if frame.get("sourceClientId") != owner_client_id:
            return False
        targets = frame.get("targetClientIds")
        if not isinstance(targets, list) or self.client_id not in targets:
            return False
        inner = frame.get("params") or {}
        if inner.get("conversationId") != conversation_id:
            return False
        if inner.get("hostId") != host_id:
            return False
        change = inner.get("change") or {}
        return change.get("type") == "snapshot"

    async def _broadcast(
        self,
        method: str,
        params: dict[str, Any],
        *,
        target_client_ids: list[str] | None = None,
    ) -> None:
        if self._writer is None:
            raise DesktopIpcUnavailable("desktop IPC client is not connected")
        message: dict[str, Any] = {
            "type": "broadcast",
            "method": method,
            "sourceClientId": self.client_id,
            "params": params,
            "version": BROADCAST_VERSIONS.get(method, 1),
        }
        if target_client_ids is not None:
            message["targetClientIds"] = target_client_ids
        payload = json.dumps(message).encode("utf-8")
        self._writer.write(struct.pack("<I", len(payload)) + payload)
        await self._writer.drain()

    async def _request(
        self,
        method: str,
        params: dict[str, Any],
        *,
        version: int | None = -1,
        target_client_id: str | None = None,
    ) -> IpcResponse:
        if self._writer is None or self._reader is None:
            raise DesktopIpcUnavailable("desktop IPC client is not connected")
        request_id = str(uuid.uuid4())
        message: dict[str, Any] = {
            "type": "request",
            "requestId": request_id,
            "method": method,
            "params": params,
        }
        if version == -1:
            version = REQUEST_VERSIONS.get(method)
        if version is not None:
            message["version"] = version
        if target_client_id is not None:
            message["targetClientId"] = target_client_id
        if self.client_id is not None:
            message["sourceClientId"] = self.client_id
        payload = json.dumps(message).encode("utf-8")
        self._writer.write(struct.pack("<I", len(payload)) + payload)
        await self._writer.drain()
        while True:
            frame = await self._read_frame()
            if frame.get("type") != "response" or frame.get("requestId") != request_id:
                continue
            return IpcResponse(
                method=frame.get("method"),
                result_type=str(frame.get("resultType")),
                result=frame.get("result"),
                error=frame.get("error"),
                handled_by_client_id=frame.get("handledByClientId"),
            )

    async def _read_frame(self) -> dict[str, Any]:
        assert self._reader is not None
        try:
            header = await asyncio.wait_for(self._reader.readexactly(4), self._timeout)
            (length,) = struct.unpack("<I", header)
            if length == 0 or length > _MAX_FRAME:
                raise DesktopIpcUnavailable(
                    f"invalid desktop IPC frame length {length}"
                )
            body = await asyncio.wait_for(
                self._reader.readexactly(length), self._timeout
            )
        except (TimeoutError, asyncio.IncompleteReadError) as error:
            raise DesktopIpcUnavailable(
                f"desktop IPC connection ended: {error}"
            ) from error
        try:
            frame = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise DesktopIpcUnavailable("desktop IPC frame is not JSON") from error
        if not isinstance(frame, dict):
            raise DesktopIpcUnavailable("desktop IPC frame is not an object")
        return frame


def conversation_is_active(state: dict[str, Any]) -> bool:
    """The desktop's own activity rule for a conversation snapshot: a
    thread awaiting resume is active iff its runtime status is active;
    otherwise a thread with no turns is active iff it is resuming, and a
    thread with turns is active iff its last turn is ``inProgress``."""

    resume_state = state.get("resumeState")
    turns = state.get("turns")
    if not isinstance(turns, list):
        turns = []
    runtime = state.get("threadRuntimeStatus")
    runtime_type = runtime.get("type") if isinstance(runtime, dict) else None
    if resume_state == "needs_resume":
        return runtime_type == "active"
    if isinstance(runtime_type, str) and not turns:
        # Live snapshots carry the runtime status without the turn list
        # (observed 2026-09-03: ``{'type': 'idle'}``); it is authoritative.
        return runtime_type != "idle"
    if not turns:
        return resume_state == "resuming"
    last = turns[-1]
    return isinstance(last, dict) and last.get("status") == "inProgress"


@dataclass(frozen=True, slots=True)
class ThreadActivity:
    owner_client_id: str | None
    active: bool | None  # None: no authoritative snapshot -> fail closed


@dataclass(frozen=True, slots=True)
class OwnerStartOutcome:
    started: bool
    owner_client_id: str | None
    reason: str


class OwnerTurnStarter:
    """Start one Sol turn through the desktop window that OWNS the thread.

    Composes, over one bounded connection: ``initialize`` ->
    ``thread-owner-discovery`` (fail closed when no window owns the
    thread) -> owner-routed ``thread-follower-start-turn`` carrying the
    wake envelope as the user message. The owner applies the thread's
    own persisted permissions (``useAppServerPermissionDefault``), so the
    turn runs under the policy Hermes set on the thread and belongs to
    the desktop app; this client disconnecting cannot interrupt it.
    """

    def __init__(
        self,
        socket_path: Path,
        *,
        cwd_for_thread: Any = None,
        host_id: str = LOCAL_HOST_ID,
        timeout: float = 15.0,
        client_factory: Any = None,
        register_owner: Any = None,
        owner_wait_seconds: float = 12.0,
    ) -> None:
        self._socket_path = socket_path
        self._cwd_for_thread = cwd_for_thread
        self._host_id = host_id
        self._timeout = timeout
        self._client_factory = client_factory or (
            lambda: DesktopIpcClient(socket_path, timeout=timeout)
        )
        # When no window owns the thread yet, ``open -g codex://threads/<id>``
        # makes the desktop app register as its owner without taking focus
        # (operator-proven 2026-09-03); discovery is then retried, bounded.
        self._register_owner = register_owner or open_thread_in_desktop
        self._owner_wait_seconds = owner_wait_seconds

    async def discover_owner(self, thread_id: str) -> str | None:
        """The desktop window owning ``thread_id`` (registering it in the
        background first when needed), or None -- one bounded connection,
        no App Server, no turn."""

        client = self._client_factory()
        try:
            await client.connect()
            owner = await client.discover_owner(thread_id, host_id=self._host_id)
            if owner is None:
                owner = await self._register_and_rediscover(client, thread_id)
            return owner
        except DesktopIpcUnavailable:
            return None
        finally:
            await client.close()

    async def thread_activity(self, thread_id: str) -> ThreadActivity:
        """Owner-routed activity proof (Sol 32f98837): the owning window's
        live snapshot decides whether the exact thread has a turn in
        progress. Owner existence alone proves nothing; no snapshot means
        ``active=None`` and callers fail closed."""

        client = self._client_factory()
        try:
            await client.connect()
            owner = await client.discover_owner(thread_id, host_id=self._host_id)
            if owner is None:
                owner = await self._register_and_rediscover(client, thread_id)
            if owner is None:
                return ThreadActivity(owner_client_id=None, active=None)
            state = await client.request_stream_snapshot(
                thread_id, owner_client_id=owner, host_id=self._host_id
            )
            if state is None:
                return ThreadActivity(owner_client_id=owner, active=None)
            return ThreadActivity(
                owner_client_id=owner, active=conversation_is_active(state)
            )
        except DesktopIpcUnavailable:
            return ThreadActivity(owner_client_id=None, active=None)
        finally:
            await client.close()

    async def start(
        self, thread_id: str, message: str, *, cwd: str | None = None
    ) -> OwnerStartOutcome:
        client = self._client_factory()
        try:
            await client.connect()
            owner = await client.discover_owner(thread_id, host_id=self._host_id)
            if owner is None:
                owner = await self._register_and_rediscover(client, thread_id)
            if owner is None:
                return OwnerStartOutcome(
                    started=False,
                    owner_client_id=None,
                    reason="owner_not_found: no desktop window owns the thread",
                )
            request: dict[str, Any] = {
                "threadId": thread_id,
                "input": [{"type": "text", "text": message, "text_elements": []}],
            }
            resolved_cwd = cwd
            if resolved_cwd is None and self._cwd_for_thread is not None:
                resolved_cwd = self._cwd_for_thread(thread_id)
            if resolved_cwd is not None:
                request["cwd"] = str(resolved_cwd)
            await client.start_turn(
                thread_id,
                owner_client_id=owner,
                turn_request=request,
                context={
                    "useAppServerPermissionDefault": True,
                    "usePermissionSelection": False,
                    "inheritThreadSettings": True,
                    "attachments": [],
                    "commentAttachments": [],
                    "mcpAppModelContextAttachments": [],
                    "localTurnMetadata": None,
                },
            )
            return OwnerStartOutcome(
                started=True, owner_client_id=owner, reason="owner_started"
            )
        except DesktopIpcUnavailable as error:
            return OwnerStartOutcome(
                started=False,
                owner_client_id=None,
                reason=f"owner_start_failed: {error}",
            )
        finally:
            await client.close()

    async def _register_and_rediscover(
        self, client: DesktopIpcClient, thread_id: str
    ) -> str | None:
        try:
            await self._register_owner(thread_id)
        except Exception:
            return None
        deadline = asyncio.get_running_loop().time() + self._owner_wait_seconds
        while True:
            owner = await client.discover_owner(thread_id, host_id=self._host_id)
            if owner is not None or asyncio.get_running_loop().time() >= deadline:
                return owner
            await asyncio.sleep(0.5)


async def open_thread_in_desktop(
    thread_id: str, *, process_factory: Any = asyncio.create_subprocess_exec
) -> None:
    """Open ``thread_id`` in its own Codex window, then let IPC prove ownership.

    A ``codex://threads`` deep link only navigates the app's current primary
    window.  An unowned second-project Merger therefore needs Codex's built-in
    ``New Window`` action first; the caller's bounded owner rediscovery remains
    the authoritative proof that the intended thread actually attached.
    """

    script = 'tell application id "com.openai.codex" to make new window\n'
    process = await process_factory("/usr/bin/osascript", "-e", script)
    code = await asyncio.wait_for(process.wait(), 10.0)
    if code != 0:
        raise DesktopIpcUnavailable(f"new Codex window exited with {code}")

    process = await process_factory(
        "/usr/bin/open", "-g", f"codex://threads/{thread_id}"
    )
    code = await asyncio.wait_for(process.wait(), 10.0)
    if code != 0:
        raise DesktopIpcUnavailable(f"open codex://threads exited with {code}")
