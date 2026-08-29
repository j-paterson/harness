"""Control a Codex App Server child process over stdio JSON-RPC."""

from __future__ import annotations

import asyncio
import json
import os
import signal
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from hermes_orchestrator.processes import ProcessRegistry, register_spawned

_CLIENT_INFO = {
    "name": "hermes_orchestrator",
    "title": "Hermes Orchestrator",
    "version": "0.1.0",
}
_MAX_LINE_BYTES = 1024 * 1024
_SCRUBBED_ENVIRONMENT_KEYS = frozenset({"OPENAI_API_KEY"})
_DEFAULT_NOTIFICATION_LIMIT = 256
_METHOD_NOT_FOUND = -32601

_STABLE_METHODS = frozenset(
    {
        "account/read",
        "account/rateLimits/read",
        "model/list",
        "review/start",
        "thread/list",
        "thread/metadata/update",
        "thread/goal/set",
        "thread/name/set",
        "thread/section/move",
        "threadSection/list",
        "thread/read",
        "thread/resume",
        "thread/start",
        "thread/unsubscribe",
        "turn/interrupt",
        "turn/start",
    }
)
_CLOSED = object()


ProcessFactory = Callable[..., Awaitable[asyncio.subprocess.Process]]


# The installed Codex application binary. The daemon runs under cmux's
# minimal shell environment where a PATH-resolved bare "codex" fails
# with exit 127, so the App Server always launches through a validated
# absolute path.
CODEX_BINARY = "/Applications/Codex.app/Contents/Resources/codex"


def app_server_command(executable: str | None = None) -> list[str]:
    """Return the stable argv that launches the App Server on stdio.

    Only an absolute executable path is accepted; a bare or relative
    name would resolve through the caller's PATH and reproduce the
    observed exit-127 launch failure under cmux's minimal environment.
    """

    resolved = executable if executable is not None else CODEX_BINARY
    if not Path(resolved).is_absolute():
        raise ValueError(
            "the codex app-server executable must be an absolute path"
        )
    return [resolved, "app-server", "--listen", "stdio://"]


class CodexUnavailable(RuntimeError):
    """Raised when the App Server cannot serve requests."""


class CodexTimeout(TimeoutError):
    """Raised when one request exceeds its explicit timeout."""


class CodexRequestFailed(RuntimeError):
    """Raised for a JSON-RPC error response without exposing its message."""

    def __init__(self, method: str, code: int | None) -> None:
        super().__init__(f"{method} failed with Codex error code {code}")
        self.method = method
        self.code = code


@dataclass(frozen=True, slots=True)
class RpcNotification:
    """One server-initiated notification."""

    method: str
    params: dict[str, Any]


@dataclass(frozen=True, slots=True)
class CodexAccount:
    """Redacted account state safe for persistence."""

    account_type: str | None
    plan_type: str | None
    requires_openai_auth: bool

    @property
    def is_chatgpt(self) -> bool:
        """Report whether the server is using ChatGPT authentication."""

        return self.account_type == "chatgpt"


@dataclass(frozen=True, slots=True)
class CodexRateLimits:
    """Snapshot of the ChatGPT rate-limit surface."""

    primary_used_percent: int | None
    secondary_used_percent: int | None
    primary_resets_at: int | None
    reached: bool


def parse_rate_limits(result: dict[str, Any]) -> CodexRateLimits:
    """Build a redacted snapshot from an account/rateLimits/read result."""

    snapshot = result.get("rateLimits")
    if not isinstance(snapshot, dict):
        snapshot = {}
    primary = snapshot.get("primary") or {}
    secondary = snapshot.get("secondary") or {}
    return CodexRateLimits(
        primary_used_percent=primary.get("usedPercent"),
        secondary_used_percent=secondary.get("usedPercent"),
        primary_resets_at=primary.get("resetsAt"),
        reached=snapshot.get("rateLimitReachedType") is not None,
    )


class CodexRpcClient:
    """Restartable stdio JSON-RPC client for the Codex App Server."""

    def __init__(
        self,
        command: Sequence[str],
        *,
        base_env: Mapping[str, str] | None = None,
        process_factory: ProcessFactory = asyncio.create_subprocess_exec,
        handshake_timeout: float = 10.0,
        termination_timeout: float = 5.0,
        notification_limit: int = _DEFAULT_NOTIFICATION_LIMIT,
        processes: ProcessRegistry | None = None,
        project_key: str = "merger",
    ) -> None:
        if not command:
            raise ValueError("codex command must not be empty")
        if handshake_timeout <= 0:
            raise ValueError("handshake timeout must be positive")
        if termination_timeout <= 0:
            raise ValueError("termination timeout must be positive")
        if notification_limit <= 0:
            raise ValueError("notification limit must be positive")
        self._command = tuple(command)
        self._base_env = dict(os.environ if base_env is None else base_env)
        self._process_factory = process_factory
        self._handshake_timeout = handshake_timeout
        self._termination_timeout = termination_timeout
        self._notification_limit = notification_limit
        self._process: asyncio.subprocess.Process | None = None
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._next_id = 0
        self._failure: str | None = "codex client not started"
        self._dropped = 0
        self._notifications: asyncio.Queue[RpcNotification | object] = (
            asyncio.Queue(maxsize=notification_limit)
        )
        self._reader_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._kill_task: asyncio.Task[None] | None = None
        self._processes = processes
        self._project_key = project_key
        self._lease_id: str | None = None

    @property
    def dropped_notifications(self) -> int:
        """Count notifications discarded because the queue was full."""

        return self._dropped

    async def start(self) -> None:
        """Launch the child and complete the initialize handshake."""

        process = self._process
        if (
            self._failure is None
            and process is not None
            and process.returncode is None
        ):
            raise RuntimeError("codex client is already running")
        await self._reap_tasks()
        if self._process is not None and self._process.returncode is None:
            await self._terminate(self._process)
        environment = {
            key: value
            for key, value in self._base_env.items()
            if key not in _SCRUBBED_ENVIRONMENT_KEYS
        }
        process = await self._process_factory(
            *self._command,
            env=environment,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
            limit=_MAX_LINE_BYTES,
        )
        if process.stdin is None or process.stdout is None or process.stderr is None:
            await self._terminate(process)
            raise RuntimeError("codex app-server pipes are unavailable")
        self._lease_id = await register_spawned(
            self._processes,
            process,
            project_key=self._project_key,
            kind="codex_app_server",
            executable=self._command[0],
            terminate=self._terminate,
        )
        self._process = process
        self._pending = {}
        self._next_id = 0
        self._failure = None
        self._dropped = 0
        self._notifications = asyncio.Queue(maxsize=self._notification_limit)
        self._stderr_task = asyncio.create_task(self._drain(process.stderr))
        self._reader_task = asyncio.create_task(self._read_loop(process))
        try:
            await self._roundtrip(
                "initialize",
                {"clientInfo": dict(_CLIENT_INFO)},
                self._handshake_timeout,
            )
        except BaseException:
            await self.close()
            raise
        self._write({"method": "initialized", "params": {}})

    async def request(
        self, method: str, params: dict[str, Any] | None, timeout: float
    ) -> dict[str, Any]:
        """Send one stable request and return its result object."""

        if method not in _STABLE_METHODS:
            raise ValueError(f"{method} is not a stable Codex App Server method")
        return await self._roundtrip(method, params, timeout)

    async def notifications(self) -> AsyncIterator[RpcNotification]:
        """Stream server notifications until the connection ends.

        The queue supports exactly one consumer per connection.
        """

        while True:
            item = await self._notifications.get()
            if item is _CLOSED:
                return
            assert isinstance(item, RpcNotification)
            yield item

    async def read_account(self, timeout: float = 10.0) -> CodexAccount:
        """Read the account type without persisting identity details."""

        result = await self.request(
            "account/read", {"refreshToken": False}, timeout
        )
        account = result.get("account")
        if not isinstance(account, dict):
            account = {}
        return CodexAccount(
            account_type=account.get("type"),
            plan_type=account.get("planType"),
            requires_openai_auth=bool(result.get("requiresOpenaiAuth")),
        )

    async def read_rate_limits(self, timeout: float = 10.0) -> CodexRateLimits:
        """Read the ChatGPT rate-limit snapshot."""

        result = await self.request("account/rateLimits/read", None, timeout)
        return parse_rate_limits(result)

    async def interrupt(
        self, thread_id: str, turn_id: str, timeout: float = 10.0
    ) -> None:
        """Interrupt one exact turn on one exact thread."""

        await self.request(
            "turn/interrupt",
            {"threadId": thread_id, "turnId": turn_id},
            timeout,
        )

    async def close(self) -> None:
        """Fail pending work and stop the child process group."""

        self._fail_connection("codex client closed")
        process = self._process
        if process is not None and process.returncode is None:
            if process.stdin is not None:
                with suppress(OSError):
                    process.stdin.close()
            try:
                await asyncio.wait_for(
                    process.wait(), timeout=self._termination_timeout
                )
            except TimeoutError:
                with suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
                await process.wait()
        self._release_lease(process)
        await self._reap_tasks()

    def _release_lease(self, process: asyncio.subprocess.Process | None) -> None:
        if self._lease_id is None or self._processes is None:
            return
        exit_code = process.returncode if process is not None else None
        with suppress(Exception):  # lease may already be settled elsewhere
            self._processes.mark_exited(self._lease_id, exit_code=exit_code)
        self._lease_id = None

    async def _roundtrip(
        self, method: str, params: dict[str, Any] | None, timeout: float
    ) -> dict[str, Any]:
        if timeout <= 0:
            raise ValueError("request timeout must be positive")
        if self._failure is not None:
            raise CodexUnavailable(self._failure)
        self._next_id += 1
        request_id = self._next_id
        future: asyncio.Future[dict[str, Any]] = (
            asyncio.get_running_loop().create_future()
        )
        self._pending[request_id] = future
        message: dict[str, Any] = {"method": method, "id": request_id}
        message["params"] = {} if params is None else params
        with suppress(OSError):
            self._write(message)
        try:
            response = await asyncio.wait_for(future, timeout=timeout)
        except TimeoutError:
            raise CodexTimeout(
                f"{method} timed out after {timeout} seconds"
            ) from None
        finally:
            self._pending.pop(request_id, None)
        if "error" in response:
            code = response["error"].get("code")
            raise CodexRequestFailed(
                method, code if isinstance(code, int) else None
            )
        return response["result"]

    def _write(self, message: dict[str, Any]) -> None:
        process = self._process
        if process is None or process.stdin is None:
            raise CodexUnavailable("codex app-server stdin is unavailable")
        line = json.dumps(message, sort_keys=True, separators=(",", ":"))
        process.stdin.write(line.encode("utf-8") + b"\n")

    async def _read_loop(self, process: asyncio.subprocess.Process) -> None:
        stdout = process.stdout
        if stdout is None:
            return
        try:
            while True:
                try:
                    line = await stdout.readline()
                except (ValueError, asyncio.LimitOverrunError):
                    self._fail_closed(
                        process, "codex protocol violation: oversized JSONL line"
                    )
                    return
                if not line:
                    code = await process.wait()
                    if self._failure is None:
                        self._fail_connection(
                            f"codex app-server exited with exit code {code}"
                        )
                    return
                try:
                    message = json.loads(line)
                except json.JSONDecodeError:
                    self._fail_closed(
                        process, "codex protocol violation: malformed JSON line"
                    )
                    return
                if not isinstance(message, dict):
                    raise ValueError("top-level message is not an object")
                self._dispatch(message)
        except Exception:
            self._fail_closed(
                process, "codex protocol violation: unprocessable server message"
            )

    def _fail_closed(
        self, process: asyncio.subprocess.Process, reason: str
    ) -> None:
        self._fail_connection(reason)
        self._kill_task = asyncio.get_running_loop().create_task(
            self._terminate(process)
        )

    def _dispatch(self, message: dict[str, Any]) -> None:
        if "method" in message and "id" in message:
            with suppress(CodexUnavailable, OSError):
                self._write(
                    {
                        "id": message["id"],
                        "error": {
                            "code": _METHOD_NOT_FOUND,
                            "message": "server-initiated requests are not supported",
                        },
                    }
                )
            return
        if "id" in message:
            identifier = message.get("id")
            if type(identifier) is not int:
                raise ValueError("response id is not an integer")
            if ("result" in message) == ("error" in message):
                raise ValueError(
                    "response must carry exactly one of result or error"
                )
            if "result" in message and not isinstance(message["result"], dict):
                raise ValueError("response result is not an object")
            if "error" in message and not isinstance(message["error"], dict):
                raise ValueError("response error is not an object")
            future = self._pending.pop(identifier, None)
            if future is not None and not future.done():
                future.set_result(message)
            return
        method = message.get("method")
        if not isinstance(method, str):
            raise ValueError("notification method is not a string")
        params = message.get("params", {})
        if not isinstance(params, dict):
            raise ValueError("notification params is not an object")
        if self._failure is not None:
            return
        self._enqueue(RpcNotification(method=method, params=params))

    def _enqueue(self, item: RpcNotification | object) -> None:
        while True:
            try:
                self._notifications.put_nowait(item)
                return
            except asyncio.QueueFull:
                with suppress(asyncio.QueueEmpty):
                    self._notifications.get_nowait()
                    self._dropped += 1

    def _fail_connection(self, reason: str) -> None:
        if self._failure is None:
            self._failure = reason
            self._enqueue(_CLOSED)
        pending = list(self._pending.values())
        self._pending.clear()
        for future in pending:
            if not future.done():
                future.set_exception(CodexUnavailable(reason))

    async def _reap_tasks(self) -> None:
        for task in (self._reader_task, self._stderr_task):
            if task is not None and not task.done():
                task.cancel()
        tasks = [
            task
            for task in (self._kill_task, self._reader_task, self._stderr_task)
            if task is not None
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._kill_task = None
        self._reader_task = None
        self._stderr_task = None

    async def _terminate(self, process: asyncio.subprocess.Process) -> None:
        if process.returncode is not None:
            return
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            await process.wait()
            return
        try:
            await asyncio.wait_for(
                process.wait(), timeout=self._termination_timeout
            )
        except TimeoutError:
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            await process.wait()

    @staticmethod
    async def _drain(stream: asyncio.StreamReader) -> None:
        while await stream.read(65536):
            pass
