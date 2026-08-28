"""Server-rendered login/logout flow tests (INFRA-176 rework, packet 1).

A phone starting with only the one-time application token must be able to
authenticate through the HTML form: GET /login renders anonymously, POST
/login rate-limits per (address, fingerprint) before constant-time
verification and issues the hardened session cookie on success, and POST
/logout requires CSRF, revokes the session, and expires the cookie. No
response ever echoes the submitted value or an internal detail, and every
redirect targets a fixed internal path.
"""

import re
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.remote.api import (
    RemoteDependencies,
    create_operations_app,
)
from hermes_orchestrator.remote.auth import (
    REMOTE_SERVICE,
    SESSION_COOKIE_NAME,
    TOKEN_ACCOUNT,
    CsrfService,
    LoginRateLimiter,
    RemoteCredentialService,
    SessionService,
)
from hermes_orchestrator.remote.commands import ConfirmationService
from hermes_orchestrator.remote.policy import RemotePolicy
from hermes_orchestrator.remote.views import FORBIDDEN_VALUE_TERMS
from tests.remote.test_auth import FakeClock, FakeKeychain
from tests.remote.test_console import (
    CLIENT_ADDRESS,
    USER_AGENT,
    FakeExecutor,
    FakeStatus,
    FakeTargets,
    hidden_field,
    sweepable_text,
)

APP_TOKEN = "application-value-issued-once-for-tests"


@dataclass
class LoginHarness:
    app: object
    sessions: SessionService
    clock: FakeClock
    clients: list[httpx.AsyncClient] = field(default_factory=list)

    def client(self, user_agent: str = USER_AGENT) -> httpx.AsyncClient:
        client = httpx.AsyncClient(
            transport=httpx.ASGITransport(self.app, client=CLIENT_ADDRESS),
            base_url="http://testserver",
            headers={"user-agent": user_agent},
        )
        self.clients.append(client)
        return client

    async def aclose(self) -> None:
        for client in self.clients:
            await client.aclose()


@pytest.fixture
def harness(tmp_path: Path):
    clock = FakeClock()
    keychain = FakeKeychain()
    keychain.create(REMOTE_SERVICE, "session-signing", "signing-secret-for-tests")
    keychain.create(REMOTE_SERVICE, TOKEN_ACCOUNT, APP_TOKEN)
    database = Database.open(tmp_path / "state.db")
    sessions = SessionService(keychain=keychain, now=clock.now)
    dependencies = RemoteDependencies(
        sessions=sessions,
        csrf=CsrfService(keychain=keychain),
        policy=RemotePolicy(),
        status=FakeStatus(),
        confirmations=ConfirmationService(
            database=database,
            events=EventStore(database),
            policy=RemotePolicy(),
            targets=FakeTargets(),
            executor=FakeExecutor(),
            now=clock.now,
        ),
        credentials=RemoteCredentialService(keychain=keychain),
        login_limiter=LoginRateLimiter(now=clock.now),
    )
    built = LoginHarness(
        app=create_operations_app(dependencies),
        sessions=sessions,
        clock=clock,
    )
    yield built
    database.close()


def issued_cookie(response: httpx.Response) -> str:
    header = response.headers.get("set-cookie", "")
    match = re.match(rf"{SESSION_COOKIE_NAME}=([^;]*)", header)
    assert match is not None, f"no session cookie in {header!r}"
    return match.group(1)


async def login(
    client: httpx.AsyncClient, passcode: str = APP_TOKEN
) -> httpx.Response:
    return await client.post("/login", data={"passcode": passcode})


async def authenticated_session(harness: LoginHarness) -> httpx.AsyncClient:
    """A client authenticated purely through the HTML login flow."""

    client = harness.client()
    response = await login(client)
    assert response.status_code == 303, response.status_code
    client.cookies.set(SESSION_COOKIE_NAME, issued_cookie(response))
    return client


# --- login form and happy path ----------------------------------------------


@pytest.mark.asyncio
async def test_login_form_renders_anonymously(harness: LoginHarness) -> None:
    try:
        response = await harness.client().get("/login")
    finally:
        await harness.aclose()

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    page = response.text
    assert '<form method="post" action="/login">' in page
    assert 'name="passcode"' in page
    assert '<label for="passcode"' in page
    assert "set-cookie" not in response.headers


@pytest.mark.asyncio
async def test_login_with_the_token_reaches_the_console(
    harness: LoginHarness,
) -> None:
    try:
        client = harness.client()
        response = await login(client)
        assert response.status_code == 303
        assert response.headers["location"] == "/console"
        client.cookies.set(SESSION_COOKIE_NAME, issued_cookie(response))
        console = await client.get("/console")
    finally:
        await harness.aclose()

    assert console.status_code == 200
    assert "Hermes operations console" in console.text


@pytest.mark.asyncio
async def test_issued_cookie_carries_the_hardened_attributes(
    harness: LoginHarness,
) -> None:
    try:
        response = await login(harness.client())
    finally:
        await harness.aclose()

    header = response.headers["set-cookie"]
    for attribute in (
        "HttpOnly",
        "Secure",
        "SameSite=Strict",
        "Path=/",
        "Max-Age=900",
    ):
        assert attribute in header, attribute


@pytest.mark.asyncio
async def test_authenticated_login_page_redirects_to_the_console(
    harness: LoginHarness,
) -> None:
    try:
        client = await authenticated_session(harness)
        response = await client.get("/login")
    finally:
        await harness.aclose()

    assert response.status_code == 303
    assert response.headers["location"] == "/console"


# --- masked credential entry --------------------------------------------------


@pytest.mark.asyncio
async def test_login_input_is_a_masked_password_manager_field(
    harness: LoginHarness,
) -> None:
    try:
        response = await harness.client().get("/login")
    finally:
        await harness.aclose()

    match = re.search(r'<input[^>]*name="passcode"[^>]*>', response.text)
    assert match is not None, "passcode input not found"
    tag = match.group(0)
    assert 'type="password"' in tag
    assert 'autocomplete="current-password"' in tag
    assert 'autocapitalize="none"' in tag
    assert 'spellcheck="false"' in tag
    assert 'type="text"' not in tag


@pytest.mark.asyncio
async def test_submitted_value_never_reaches_failure_html_or_headers(
    harness: LoginHarness,
) -> None:
    sentinel = "hunter2-sentinel-value"
    try:
        client = harness.client()
        failed = await login(client, sentinel)
        extra = await client.post(
            "/login", data={"passcode": sentinel, "sneaky": "x"}
        )
        for _ in range(4):
            await login(client, sentinel)
        limited = await login(client, sentinel)
    finally:
        await harness.aclose()

    assert failed.status_code == 401
    assert extra.status_code == 400
    assert limited.status_code == 429
    for response in (failed, extra, limited):
        assert sentinel not in response.text, response.status_code
        for name, value in response.headers.items():
            assert sentinel not in value, (response.status_code, name)


@pytest.mark.asyncio
async def test_login_never_flows_through_the_url(harness: LoginHarness) -> None:
    try:
        response = await harness.client().get(
            "/login?passcode=hunter2-query-value"
        )
    finally:
        await harness.aclose()

    assert response.status_code == 200
    assert "hunter2" not in response.text
    form = re.search(r'<form[^>]*action="/login"[^>]*>', response.text)
    assert form is not None
    assert 'method="post"' in form.group(0)


# --- rejection without echo --------------------------------------------------


@pytest.mark.asyncio
async def test_wrong_value_is_rejected_without_echo(harness: LoginHarness) -> None:
    try:
        client = harness.client()
        response = await login(client, "hunter2-wrong-value")
        console = await client.get("/console")
    finally:
        await harness.aclose()

    assert response.status_code == 401
    assert "hunter2" not in response.text
    assert "set-cookie" not in response.headers
    assert console.status_code == 401


@pytest.mark.asyncio
async def test_extra_login_form_fields_are_rejected(harness: LoginHarness) -> None:
    try:
        response = await harness.client().post(
            "/login", data={"passcode": APP_TOKEN, "sneaky": "hunter2-value"}
        )
    finally:
        await harness.aclose()

    assert response.status_code == 400
    assert "invalid_request" in response.text
    assert "hunter2" not in response.text
    assert "set-cookie" not in response.headers


# --- rate limiting ------------------------------------------------------------


@pytest.mark.asyncio
async def test_five_failures_lock_the_client_out_for_fifteen_minutes(
    harness: LoginHarness,
) -> None:
    try:
        client = harness.client()
        for _ in range(5):
            failed = await login(client, "wrong-value")
            assert failed.status_code == 401
        locked = await login(client)
        harness.clock.advance(minutes=15, seconds=1)
        recovered = await login(client)
    finally:
        await harness.aclose()

    assert locked.status_code == 429
    assert "rate_limited" in locked.text
    assert "set-cookie" not in locked.headers
    assert recovered.status_code == 303


@pytest.mark.asyncio
async def test_lockout_is_scoped_to_address_and_fingerprint(
    harness: LoginHarness,
) -> None:
    try:
        locked_client = harness.client()
        for _ in range(5):
            await login(locked_client, "wrong-value")
        locked = await login(locked_client)
        other = await login(harness.client(user_agent="other-operator"))
    finally:
        await harness.aclose()

    assert locked.status_code == 429
    assert other.status_code == 303


# --- logout -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_logout_requires_csrf(harness: LoginHarness) -> None:
    try:
        client = await authenticated_session(harness)
        missing = await client.post("/logout", data={})
        wrong = await client.post("/logout", data={"csrf": "nonce.not-a-mac"})
        still = await client.get("/console")
    finally:
        await harness.aclose()

    assert missing.status_code == 403
    assert wrong.status_code == 403
    assert "csrf_rejected" in wrong.text
    assert still.status_code == 200


@pytest.mark.asyncio
async def test_logout_revokes_clears_and_blocks(harness: LoginHarness) -> None:
    try:
        client = await authenticated_session(harness)
        console = await client.get("/console")
        response = await client.post(
            "/logout", data={"csrf": hidden_field(console.text, "csrf")}
        )
        after = await client.get("/console")
    finally:
        await harness.aclose()

    assert response.status_code == 303
    assert response.headers["location"] == "/login"
    header = response.headers["set-cookie"]
    assert header.startswith(f"{SESSION_COOKIE_NAME}=;")
    assert "Max-Age=0" in header
    assert after.status_code == 401


@pytest.mark.asyncio
async def test_anonymous_logout_is_unauthorized(harness: LoginHarness) -> None:
    try:
        response = await harness.client().post(
            "/logout", data={"csrf": "nonce.mac"}
        )
    finally:
        await harness.aclose()

    assert response.status_code == 401
    assert "unauthorized" in response.text


# --- damaged cookies ----------------------------------------------------------


@pytest.mark.asyncio
async def test_expired_tampered_and_wrong_client_cookies_are_rejected(
    harness: LoginHarness,
) -> None:
    try:
        expired = await authenticated_session(harness)
        harness.clock.advance(seconds=900)
        expired_response = await expired.get("/console")

        tampered = harness.client()
        value = issued_cookie(await login(harness.client()))
        tampered.cookies.set(SESSION_COOKIE_NAME, value[:-2] + "xx")
        tampered_response = await tampered.get("/console")

        stolen = harness.client(user_agent="other-operator")
        stolen.cookies.set(SESSION_COOKIE_NAME, value)
        stolen_response = await stolen.get("/console")
    finally:
        await harness.aclose()

    for response in (expired_response, tampered_response, stolen_response):
        assert response.status_code == 401
        assert "unauthorized" in response.text
        assert "mismatch" not in response.text
        assert "malformed" not in response.text


# --- leakage sweep ------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_forbidden_term_appears_in_login_or_logout_pages(
    harness: LoginHarness,
) -> None:
    try:
        client = await authenticated_session(harness)
        console = await client.get("/console")
        responses = [
            await harness.client().get("/login"),
            await login(harness.client(), "wrong-value"),
            await harness.client().post(
                "/login", data={"passcode": APP_TOKEN, "sneaky": "x"}
            ),
            await client.post("/logout", data={"csrf": "nonce.not-a-mac"}),
            await client.post(
                "/logout", data={"csrf": hidden_field(console.text, "csrf")}
            ),
            await harness.client().post("/logout", data={"csrf": "nonce.mac"}),
        ]
        limited = harness.client()
        for _ in range(5):
            await login(limited, "wrong-value")
        responses.append(await login(limited))
    finally:
        await harness.aclose()

    for response in responses:
        lowered = sweepable_text(response)
        for term in FORBIDDEN_VALUE_TERMS:
            assert term not in lowered, (response.status_code, term)
