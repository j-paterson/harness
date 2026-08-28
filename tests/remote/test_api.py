"""Loopback-only operations API tests (INFRA-175).

The app is exercised in-process through httpx's ASGI transport — no real
socket is ever bound; the loopback guard is tested via configuration
alone. Requests run on the test's own event loop so the temp sqlite
database keeps its same-thread ownership. Auth uses the INFRA-173 fakes,
status uses a canned sanitized summary, and commands run against a temp
sqlite database with a recording executor.
"""

from dataclasses import dataclass, field
from pathlib import Path

import httpx
import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.hermes_tools import HermesCommandResult
from hermes_orchestrator.remote.api import (
    BindHostNotLoopback,
    RemoteDependencies,
    create_operations_app,
)
from hermes_orchestrator.remote.auth import (
    REMOTE_SERVICE,
    SESSION_COOKIE_NAME,
    CsrfService,
    SessionService,
)
from hermes_orchestrator.remote.commands import ConfirmationService
from hermes_orchestrator.remote.policy import RemotePolicy
from hermes_orchestrator.remote.views import (
    FORBIDDEN_VALUE_TERMS,
    OperationsSummary,
    ProjectView,
)
from tests.remote.test_auth import FakeClock, FakeKeychain

CLIENT_ADDRESS = ("127.0.0.1", 9999)
USER_AGENT = "operator-console"
FINGERPRINT = f"{CLIENT_ADDRESS[0]}|{USER_AGENT}"

EXPECTED_HEADERS = {
    "cache-control": "no-store",
    "content-security-policy": (
        "default-src 'self'; frame-ancestors 'none'; form-action 'self'"
    ),
    "x-content-type-options": "nosniff",
    "referrer-policy": "no-referrer",
    "permissions-policy": (
        "camera=(), microphone=(), geolocation=(), payment=()"
    ),
}


class FakeStatus:
    """Sanitized summary provider standing in for StatusViewService."""

    def summary(self) -> OperationsSummary:
        return OperationsSummary(
            projects=(
                ProjectView(project_alias="demo", state="active", age_seconds=4),
            ),
            workers=(),
            queue=(),
            resources=None,
            reviews=(),
            stalls=(),
        )


class FakeTargets:
    def exists(self, kind: str, name: str) -> bool:
        return (kind, name) == ("project", "demo")


class SimulatedCrash(RuntimeError):
    """Stands in for a process death at the mutation boundary."""


class FakeExecutor:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.crash_next = False

    def supports(self, intent: str) -> bool:
        return True

    def execute(self, raw: object) -> HermesCommandResult:
        self.calls.append(dict(raw))
        if self.crash_next:
            self.crash_next = False
            raise SimulatedCrash("process died before persistence")
        return HermesCommandResult(
            f"corr-{len(self.calls)}", "accepted", {"outcome": "applied"}
        )


@dataclass
class Harness:
    app: object
    sessions: SessionService
    csrf_service: CsrfService
    clock: FakeClock
    executor: FakeExecutor
    database: Database
    clients: list[httpx.AsyncClient] = field(default_factory=list)

    def _client(self) -> httpx.AsyncClient:
        client = httpx.AsyncClient(
            transport=httpx.ASGITransport(self.app, client=CLIENT_ADDRESS),
            base_url="http://testserver",
            headers={"user-agent": USER_AGENT},
        )
        self.clients.append(client)
        return client

    def anonymous_client(self) -> httpx.AsyncClient:
        return self._client()

    def authenticated_client(self) -> httpx.AsyncClient:
        client = self._client()
        cookie = self.sessions.create(FINGERPRINT)
        claims = self.sessions.verify(cookie.value, FINGERPRINT)
        client.cookies.set(SESSION_COOKIE_NAME, cookie.value)
        client.csrf = self.csrf_service.issue(claims.session_id)
        client.executor = self.executor
        return client

    async def aclose(self) -> None:
        for client in self.clients:
            await client.aclose()


@pytest.fixture
def harness(tmp_path: Path):
    clock = FakeClock()
    keychain = FakeKeychain()
    keychain.create(REMOTE_SERVICE, "session-signing", "signing-secret-for-tests")
    database = Database.open(tmp_path / "state.db")
    executor = FakeExecutor()
    confirmations = ConfirmationService(
        database=database,
        events=EventStore(database),
        policy=RemotePolicy(),
        targets=FakeTargets(),
        executor=executor,
        now=clock.now,
    )
    dependencies = RemoteDependencies(
        sessions=SessionService(keychain=keychain, now=clock.now),
        csrf=CsrfService(keychain=keychain),
        policy=RemotePolicy(),
        status=FakeStatus(),
        confirmations=confirmations,
    )
    built = Harness(
        app=create_operations_app(dependencies),
        sessions=dependencies.sessions,
        csrf_service=dependencies.csrf,
        clock=clock,
        executor=executor,
        database=database,
    )
    yield built
    database.close()


async def prepare_pause(client: httpx.AsyncClient) -> dict:
    response = await client.post(
        "/api/commands/prepare",
        json={"intent": "pause", "target": "project:demo"},
        headers={"x-csrf-token": client.csrf},
    )
    assert response.status_code == 200
    return response.json()


async def confirm_pause(
    client: httpx.AsyncClient, idempotency_key: str
) -> httpx.Response:
    pending = await prepare_pause(client)
    return await client.post(
        "/api/commands/confirm",
        json={
            "confirmation_id": pending["confirmation_id"],
            "confirmation_phrase": "PAUSE DEMO",
            "idempotency_key": idempotency_key,
        },
        headers={"x-csrf-token": client.csrf},
    )


# --- authentication ---------------------------------------------------------


@pytest.mark.asyncio
async def test_status_requires_authentication(harness: Harness) -> None:
    try:
        response = await harness.anonymous_client().get("/api/status")
    finally:
        await harness.aclose()

    assert response.status_code == 401
    assert response.json() == {"code": "unauthorized"}


@pytest.mark.asyncio
async def test_expired_session_is_unauthorized(harness: Harness) -> None:
    try:
        client = harness.authenticated_client()
        harness.clock.advance(seconds=900)
        response = await client.get("/api/status")
    finally:
        await harness.aclose()

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_mutations_require_authentication(harness: Harness) -> None:
    try:
        response = await harness.anonymous_client().post(
            "/api/commands/prepare",
            json={"intent": "pause", "target": "project:demo"},
        )
    finally:
        await harness.aclose()

    assert response.status_code == 401


# --- status -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_authenticated_status_is_no_store(harness: Harness) -> None:
    try:
        response = await harness.authenticated_client().get("/api/status")
    finally:
        await harness.aclose()

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"


@pytest.mark.asyncio
async def test_status_renders_only_sanitized_view_fields(harness: Harness) -> None:
    try:
        body = (await harness.authenticated_client().get("/api/status")).json()
    finally:
        await harness.aclose()

    assert body["projects"] == [
        {"project_alias": "demo", "state": "active", "age_seconds": 4}
    ]
    assert set(body) == {
        "projects",
        "workers",
        "queue",
        "resources",
        "reviews",
        "stalls",
    }


# --- headers and bind guard -------------------------------------------------


@pytest.mark.asyncio
async def test_security_headers_are_present_on_every_response(
    harness: Harness,
) -> None:
    try:
        anonymous = harness.anonymous_client()
        authenticated = harness.authenticated_client()
        responses = [
            await anonymous.get("/api/status"),
            await authenticated.get("/api/status"),
            await anonymous.get("/api/definitely-not-a-route"),
            await authenticated.post(
                "/api/commands/confirm",
                json={"confirmation_id": 7},
                headers={"x-csrf-token": authenticated.csrf},
            ),
        ]
    finally:
        await harness.aclose()

    for response in responses:
        for header, value in EXPECTED_HEADERS.items():
            assert response.headers.get(header) == value, (
                response.request.url,
                header,
            )


def test_non_loopback_bind_hosts_are_rejected_at_startup(harness: Harness) -> None:
    for bad_host in ("0.0.0.0", "localhost", "192.168.1.4", "::1"):
        with pytest.raises(BindHostNotLoopback):
            create_operations_app(
                RemoteDependencies(
                    sessions=harness.sessions,
                    csrf=harness.csrf_service,
                    policy=RemotePolicy(),
                    status=FakeStatus(),
                    confirmations=None,
                    bind_host=bad_host,
                )
            )


# --- CSRF -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mutations_without_csrf_are_rejected(harness: Harness) -> None:
    try:
        client = harness.authenticated_client()
        missing = await client.post(
            "/api/commands/prepare",
            json={"intent": "pause", "target": "project:demo"},
        )
        wrong = await client.post(
            "/api/commands/prepare",
            json={"intent": "pause", "target": "project:demo"},
            headers={"x-csrf-token": "nonce.not-a-real-mac"},
        )
    finally:
        await harness.aclose()

    assert missing.status_code == 403
    assert wrong.status_code == 403
    assert missing.json() == {"code": "csrf_rejected"}
    assert harness.executor.calls == []


@pytest.mark.asyncio
async def test_csrf_from_another_session_is_rejected(harness: Harness) -> None:
    try:
        first = harness.authenticated_client()
        second = harness.authenticated_client()
        response = await first.post(
            "/api/commands/prepare",
            json={"intent": "pause", "target": "project:demo"},
            headers={"x-csrf-token": second.csrf},
        )
    finally:
        await harness.aclose()

    assert response.status_code == 403


# --- confirmation flow ------------------------------------------------------


@pytest.mark.asyncio
async def test_mutation_requires_csrf_and_confirmation(harness: Harness) -> None:
    try:
        client = harness.authenticated_client()
        pending = await prepare_pause(client)
        assert pending["confirmation_phrase"] == "PAUSE DEMO"
        denied = await client.post(
            "/api/commands/confirm",
            json={
                "confirmation_id": pending["confirmation_id"],
                "confirmation_phrase": "wrong",
                "idempotency_key": "key-1",
            },
            headers={"x-csrf-token": client.csrf},
        )
    finally:
        await harness.aclose()

    assert denied.status_code == 409
    assert harness.executor.calls == []


@pytest.mark.asyncio
async def test_retried_confirm_with_same_key_executes_once(harness: Harness) -> None:
    try:
        client = harness.authenticated_client()
        pending = await prepare_pause(client)
        payload = {
            "confirmation_id": pending["confirmation_id"],
            "confirmation_phrase": "PAUSE DEMO",
            "idempotency_key": "same-key",
        }
        headers = {"x-csrf-token": client.csrf}
        first = await client.post(
            "/api/commands/confirm", json=payload, headers=headers
        )
        second = await client.post(
            "/api/commands/confirm", json=payload, headers=headers
        )
    finally:
        await harness.aclose()

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json() == second.json()
    assert len(harness.executor.calls) == 1


@pytest.mark.asyncio
async def test_key_reuse_for_a_different_confirmation_conflicts(
    harness: Harness,
) -> None:
    try:
        client = harness.authenticated_client()
        first = await confirm_pause(client, "same-key")
        second = await confirm_pause(client, "same-key")
    finally:
        await harness.aclose()

    assert first.status_code == 200
    assert second.status_code == 409
    assert second.json() == {"code": "idempotency_key_conflict"}
    assert len(harness.executor.calls) == 1


@pytest.mark.asyncio
async def test_unresolved_claimed_execution_fails_closed(harness: Harness) -> None:
    try:
        client = harness.authenticated_client()
        pending = await prepare_pause(client)
        payload = {
            "confirmation_id": pending["confirmation_id"],
            "confirmation_phrase": "PAUSE DEMO",
            "idempotency_key": "key-1",
        }
        headers = {"x-csrf-token": client.csrf}
        harness.executor.crash_next = True
        with pytest.raises(SimulatedCrash):
            await client.post(
                "/api/commands/confirm", json=payload, headers=headers
            )
        retry = await client.post(
            "/api/commands/confirm", json=payload, headers=headers
        )
    finally:
        await harness.aclose()

    assert retry.status_code == 409
    assert retry.json() == {"code": "execution_unresolved"}
    assert len(harness.executor.calls) == 1


@pytest.mark.asyncio
async def test_expired_confirmation_is_rejected(harness: Harness) -> None:
    try:
        client = harness.authenticated_client()
        pending = await prepare_pause(client)
        harness.clock.advance(seconds=121)
        response = await client.post(
            "/api/commands/confirm",
            json={
                "confirmation_id": pending["confirmation_id"],
                "confirmation_phrase": "PAUSE DEMO",
                "idempotency_key": "key-1",
            },
            headers={"x-csrf-token": client.csrf},
        )
    finally:
        await harness.aclose()

    assert response.status_code == 410
    assert harness.executor.calls == []


@pytest.mark.asyncio
async def test_replayed_confirmation_is_rejected(harness: Harness) -> None:
    try:
        client = harness.authenticated_client()
        pending = await prepare_pause(client)
        payload = {
            "confirmation_id": pending["confirmation_id"],
            "confirmation_phrase": "PAUSE DEMO",
            "idempotency_key": "key-1",
        }
        headers = {"x-csrf-token": client.csrf}
        first = await client.post(
            "/api/commands/confirm", json=payload, headers=headers
        )
        replayed = await client.post(
            "/api/commands/confirm",
            json={**payload, "idempotency_key": "key-2"},
            headers=headers,
        )
    finally:
        await harness.aclose()

    assert first.status_code == 200
    assert replayed.status_code == 409
    assert len(harness.executor.calls) == 1


@pytest.mark.asyncio
async def test_confirming_another_sessions_command_is_unknown(
    harness: Harness,
) -> None:
    try:
        owner = harness.authenticated_client()
        intruder = harness.authenticated_client()
        pending = await prepare_pause(owner)
        response = await intruder.post(
            "/api/commands/confirm",
            json={
                "confirmation_id": pending["confirmation_id"],
                "confirmation_phrase": "PAUSE DEMO",
                "idempotency_key": "key-1",
            },
            headers={"x-csrf-token": intruder.csrf},
        )
    finally:
        await harness.aclose()

    assert response.status_code == 404
    assert harness.executor.calls == []


# --- policy and validation --------------------------------------------------


@pytest.mark.asyncio
async def test_forbidden_intent_is_denied_and_never_executes(
    harness: Harness,
) -> None:
    try:
        client = harness.authenticated_client()
        response = await client.post(
            "/api/commands/prepare",
            json={"intent": "shell", "target": "project:demo"},
            headers={"x-csrf-token": client.csrf},
        )
    finally:
        await harness.aclose()

    assert response.status_code == 403
    assert response.json() == {"code": "forbidden_intent"}
    assert harness.executor.calls == []


@pytest.mark.asyncio
async def test_unknown_intent_is_a_client_error_not_a_crash(
    harness: Harness,
) -> None:
    try:
        client = harness.authenticated_client()
        response = await client.post(
            "/api/commands/prepare",
            json={"intent": "frobnicate", "target": "project:demo"},
            headers={"x-csrf-token": client.csrf},
        )
    finally:
        await harness.aclose()

    assert response.status_code == 400
    assert response.json() == {"code": "invalid_intent"}


@pytest.mark.asyncio
async def test_unknown_target_is_rejected(harness: Harness) -> None:
    try:
        client = harness.authenticated_client()
        response = await client.post(
            "/api/commands/prepare",
            json={"intent": "pause", "target": "project:ghost"},
            headers={"x-csrf-token": client.csrf},
        )
    finally:
        await harness.aclose()

    assert response.status_code == 404
    assert response.json() == {"code": "unknown_target"}


@pytest.mark.asyncio
async def test_validation_errors_never_echo_the_input(harness: Harness) -> None:
    try:
        client = harness.authenticated_client()
        response = await client.post(
            "/api/commands/confirm",
            json={"confirmation_id": {"sneaky": "hunter2-value"}},
            headers={"x-csrf-token": client.csrf},
        )
    finally:
        await harness.aclose()

    assert response.status_code == 400
    assert response.json() == {"code": "invalid_request"}
    assert "hunter2" not in response.text


# --- redaction --------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_forbidden_term_appears_in_any_response_body(
    harness: Harness,
) -> None:
    try:
        anonymous = harness.anonymous_client()
        client = harness.authenticated_client()
        csrf_headers = {"x-csrf-token": client.csrf}
        responses = [
            await anonymous.get("/api/status"),
            await client.get("/api/status"),
            await client.post(
                "/api/commands/prepare",
                json={"intent": "pause", "target": "project:demo"},
                headers=csrf_headers,
            ),
            await client.post(
                "/api/commands/prepare",
                json={"intent": "credentials", "target": "project:demo"},
                headers=csrf_headers,
            ),
            await client.post(
                "/api/commands/prepare",
                json={"intent": "pause", "target": "project:ghost"},
                headers=csrf_headers,
            ),
            await client.post(
                "/api/commands/confirm",
                json={"confirmation_id": 7},
                headers=csrf_headers,
            ),
            await confirm_pause(client, "clean-key"),
            await anonymous.post(
                "/api/commands/prepare",
                json={"intent": "pause", "target": "project:demo"},
            ),
        ]
    finally:
        await harness.aclose()

    for response in responses:
        lowered = response.text.lower()
        for term in FORBIDDEN_VALUE_TERMS:
            assert term not in lowered, (response.request.url, term)


# --- intent-specific parameters (packet 2) ----------------------------------


@pytest.mark.asyncio
async def test_prepare_accepts_intent_specific_parameters(harness: Harness) -> None:
    try:
        client = harness.authenticated_client()
        response = await client.post(
            "/api/commands/prepare",
            json={
                "intent": "queue_issue",
                "target": "project:demo",
                "parameters": {
                    "issue_id": "ENG-7",
                    "priority": 2,
                    "operator_instruction_id": "chat-7",
                },
            },
            headers={"x-csrf-token": client.csrf},
        )
    finally:
        await harness.aclose()

    assert response.status_code == 200
    assert response.json()["confirmation_phrase"] == "QUEUE_ISSUE DEMO"


@pytest.mark.asyncio
async def test_missing_required_parameters_are_a_static_code(
    harness: Harness,
) -> None:
    try:
        client = harness.authenticated_client()
        response = await client.post(
            "/api/commands/prepare",
            json={
                "intent": "queue_issue",
                "target": "project:demo",
                "parameters": {"issue_id": "hunter2-issue"},
            },
            headers={"x-csrf-token": client.csrf},
        )
    finally:
        await harness.aclose()

    assert response.status_code == 400
    assert response.json() == {"code": "invalid_parameters"}
    assert "hunter2" not in response.text
    assert harness.executor.calls == []


@pytest.mark.asyncio
async def test_incompatible_intent_target_pair_is_a_static_code(
    harness: Harness,
) -> None:
    try:
        client = harness.authenticated_client()
        response = await client.post(
            "/api/commands/prepare",
            json={"intent": "retry", "target": "project:demo"},
            headers={"x-csrf-token": client.csrf},
        )
    finally:
        await harness.aclose()

    assert response.status_code == 400
    assert response.json() == {"code": "incompatible_target"}
    assert harness.executor.calls == []


@pytest.mark.asyncio
async def test_status_cannot_be_prepared_as_a_mutation(harness: Harness) -> None:
    try:
        client = harness.authenticated_client()
        response = await client.post(
            "/api/commands/prepare",
            json={"intent": "status", "target": "project:demo"},
            headers={"x-csrf-token": client.csrf},
        )
    finally:
        await harness.aclose()

    assert response.status_code == 400
    assert response.json() == {"code": "unsupported_intent"}
    assert harness.executor.calls == []
