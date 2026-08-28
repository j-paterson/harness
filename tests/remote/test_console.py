"""Server-rendered operations console tests (INFRA-176).

The console is a thin HTML presentation layer over the existing INFRA-173
auth, INFRA-174 sanitized views, and INFRA-175 confirmed mutations. These
tests drive the app in-process through httpx's ASGI transport — never a
real socket, browser, or TestClient — and assert over the rendered markup:
accessibility (viewport, labels, landmarks, touch-target sizing), mobile
rendering, security (auth on every route, CSRF on every form POST, no
forbidden-term leakage, escaping, static codes), and the full
prepare -> typed phrase -> confirm interaction for every console action.
"""

import json
import re
from base64 import urlsafe_b64decode
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.hermes_tools import HermesCommandResult
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
from hermes_orchestrator.remote.views import (
    FORBIDDEN_VALUE_TERMS,
    ContextBand,
    OperationsSummary,
    ProjectView,
    QueueItemView,
    ResourceBand,
    ResourceView,
    ReviewInput,
    ReviewView,
    StallView,
    StatusViewService,
    WorkerView,
)
from tests.remote.test_auth import FakeClock, FakeKeychain

CLIENT_ADDRESS = ("127.0.0.1", 9999)
USER_AGENT = "operator-console"
FINGERPRINT = f"{CLIENT_ADDRESS[0]}|{USER_AGENT}"

VIEWPORT_META = '<meta name="viewport" content="width=device-width, initial-scale=1">'

# Every console action: slug, target name, extra form fields, expected
# phrase, executed intent, and the strict payload identity field/value.
MUTATION_ACTIONS = [
    ("pause", "demo", {}, "PAUSE DEMO", "pause", "project_key", "demo"),
    ("resume", "demo", {}, "RESUME DEMO", "resume", "project_key", "demo"),
    ("retry", "task-1", {}, "RETRY TASK-1", "retry", "issue_id", "task-1"),
    (
        "reprioritize",
        "task-1",
        {"priority": "2"},
        "REPRIORITIZE TASK-1",
        "reprioritize",
        "issue_id",
        "task-1",
    ),
    (
        "checkpoint",
        "demo",
        {},
        "REQUEST_CHECKPOINT DEMO",
        "request_checkpoint",
        "project_key",
        "demo",
    ),
    (
        "cleanup",
        "demo",
        {},
        "REQUEST_CLEANUP DEMO",
        "request_cleanup",
        "project_key",
        "demo",
    ),
    (
        "approve-stall",
        "demo",
        {},
        "APPROVE_STALL DEMO",
        "approve_stall",
        "project_key",
        "demo",
    ),
    (
        "approve-handoff",
        "relay-1",
        {},
        "APPROVE_HANDOFF RELAY-1",
        "approve_handoff",
        "handoff_id",
        "relay-1",
    ),
    (
        "queue",
        "demo",
        {"issue_id": "task-9", "priority": "2"},
        "QUEUE_ISSUE DEMO",
        "queue_issue",
        "project_key",
        "demo",
    ),
]

ACTION_SLUGS = [action[0] for action in MUTATION_ACTIONS]

# The per-project repositories the test wiring configures for review-link
# provenance and the exact canonical pull request URLs the fixtures carry.
# The two aliases deliberately map to DIFFERENT repositories, neither of
# them this project's real repository: the tests must prove the approved
# link is configuration-driven per project, never a hard-coded literal.
CONFIGURED_GITHUB_REPOS = {
    "demo": "acme/widgets",
    "orbit": "nova/lanterns",
}
CANONICAL_PR_URL = "https://github.com/acme/widgets/pull/7"
MARKER_PR_URL = "https://github.com/nova/lanterns/pull/88"


def sanitized_summary(project_alias: str = "demo") -> OperationsSummary:
    return OperationsSummary(
        projects=(
            ProjectView(project_alias=project_alias, state="active", age_seconds=4),
        ),
        workers=(
            WorkerView(
                worker_alias="lead-1",
                profile_alias="max-a",
                project_alias="demo",
                state="working",
                age_seconds=30,
                context_band=ContextBand.OK,
            ),
        ),
        queue=(
            QueueItemView(
                issue_alias="task-1",
                project_alias="demo",
                state="queued",
                age_seconds=12,
            ),
        ),
        resources=ResourceView(
            band=ResourceBand.GREEN,
            available_memory_gib=24.0,
            total_memory_gib=64.0,
            load_one=2.5,
            logical_cpus=12,
            disk_free_gib=310.0,
        ),
        reviews=(
            ReviewView(
                issue_alias="task-2",
                project_alias="demo",
                state="in_review",
                pr_url=CANONICAL_PR_URL,
                ci_status="green",
                age_seconds=90,
            ),
        ),
        stalls=(
            StallView(
                worker_alias="lead-1",
                project_alias="demo",
                stall_summary="waiting on review",
                age_seconds=200,
                checkpoint_eligible=True,
            ),
        ),
    )


class FakeStatus:
    """Configurable sanitized summary standing in for StatusViewService."""

    def __init__(self) -> None:
        self.summary_value = sanitized_summary()
        # When set, every read delegates to a real provider (for example a
        # StatusViewService) so its fail-closed behavior is exercised
        # through the console end to end.
        self.provider = None

    def summary(self) -> OperationsSummary:
        if self.provider is not None:
            return self.provider.summary()
        return self.summary_value


class FakeTargets:
    _KNOWN = frozenset(
        {
            ("project", "demo"),
            ("issue", "task-1"),
            ("handoff", "relay-1"),
        }
    )

    def exists(self, kind: str, name: str) -> bool:
        return (kind, name) in self._KNOWN


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
    status: FakeStatus
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
        client.cookies.set(SESSION_COOKIE_NAME, cookie.value)
        return client

    async def aclose(self) -> None:
        for client in self.clients:
            await client.aclose()


@pytest.fixture
def harness(tmp_path: Path):
    clock = FakeClock()
    keychain = FakeKeychain()
    keychain.create(REMOTE_SERVICE, "session-signing", "signing-secret-for-tests")
    keychain.create(REMOTE_SERVICE, TOKEN_ACCOUNT, "application-value-for-tests")
    database = Database.open(tmp_path / "state.db")
    executor = FakeExecutor()
    status = FakeStatus()
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
        status=status,
        confirmations=confirmations,
        credentials=RemoteCredentialService(keychain=keychain),
        login_limiter=LoginRateLimiter(now=clock.now),
    )
    built = Harness(
        app=create_operations_app(dependencies),
        sessions=dependencies.sessions,
        csrf_service=dependencies.csrf,
        clock=clock,
        executor=executor,
        status=status,
        database=database,
    )
    yield built
    database.close()


# The correction mandates these exact literals on the login field while
# 'password' stays a forbidden term everywhere else, so sweeps strip each of
# them at most once from login markup only; any other occurrence — and any
# submitted value — still trips.
LOGIN_FIELD_ALLOWANCES = ('type="password"', 'autocomplete="current-password"')


def sweepable_text(response: httpx.Response) -> str:
    """Lowercased response text for forbidden-term sweeps."""

    text = response.text
    if response.request.url.path == "/login":
        for literal in LOGIN_FIELD_ALLOWANCES:
            text = text.replace(literal, "", 1)
    return text.lower()


def client_session_id(client: httpx.AsyncClient) -> str:
    """The session id inside the client's cookie; claims are compact JSON."""

    payload = client.cookies.get(SESSION_COOKIE_NAME).split(".")[0]
    padded = payload + "=" * (-len(payload) % 4)
    return str(json.loads(urlsafe_b64decode(padded.encode()))["session_id"])


def hidden_field(html: str, name: str) -> str:
    match = re.search(rf'name="{name}" value="([^"]*)"', html)
    assert match is not None, f"hidden field {name!r} not found in page"
    return match.group(1)


async def get_form(client: httpx.AsyncClient, action: str) -> httpx.Response:
    return await client.get(f"/console/actions/{action}")


async def prepare_action(
    client: httpx.AsyncClient,
    action: str,
    target: str,
    extra: dict | None = None,
) -> httpx.Response:
    form_page = await get_form(client, action)
    assert form_page.status_code == 200, (action, form_page.status_code)
    csrf = hidden_field(form_page.text, "csrf")
    return await client.post(
        f"/console/actions/{action}/prepare",
        data={"target": target, "csrf": csrf, **(extra or {})},
    )


async def confirm_action(
    client: httpx.AsyncClient,
    action: str,
    confirm_page_html: str,
    phrase: str,
    idempotency_key: str | None = None,
) -> httpx.Response:
    key = (
        idempotency_key
        if idempotency_key is not None
        else hidden_field(confirm_page_html, "idempotency_key")
    )
    return await client.post(
        f"/console/actions/{action}/confirm",
        data={
            "confirmation_id": hidden_field(confirm_page_html, "confirmation_id"),
            "confirmation_phrase": phrase,
            "idempotency_key": key,
            "csrf": hidden_field(confirm_page_html, "csrf"),
        },
    )


async def run_action(
    client: httpx.AsyncClient,
    action: str,
    target: str,
    phrase: str,
    extra: dict | None = None,
) -> httpx.Response:
    prepared = await prepare_action(client, action, target, extra)
    assert prepared.status_code == 200, (action, prepared.status_code)
    return await confirm_action(client, action, prepared.text, phrase)


# --- authentication ---------------------------------------------------------


@pytest.mark.asyncio
async def test_every_console_route_requires_authentication(
    harness: Harness,
) -> None:
    try:
        client = harness.anonymous_client()
        responses = [
            await client.get("/console"),
            await client.get("/console/actions/pause"),
            await client.post(
                "/console/actions/pause/prepare",
                data={"target": "demo", "csrf": "nonce.mac"},
            ),
            await client.post(
                "/console/actions/pause/confirm",
                data={
                    "confirmation_id": "cid",
                    "confirmation_phrase": "PAUSE DEMO",
                    "idempotency_key": "k",
                    "csrf": "nonce.mac",
                },
            ),
        ]
    finally:
        await harness.aclose()

    for response in responses:
        assert response.status_code == 401, response.request.url
        assert "unauthorized" in response.text


@pytest.mark.asyncio
async def test_expired_session_cannot_use_the_console(harness: Harness) -> None:
    try:
        client = harness.authenticated_client()
        harness.clock.advance(seconds=900)
        response = await client.get("/console")
    finally:
        await harness.aclose()

    assert response.status_code == 401


# --- CSRF -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_form_post_without_csrf_is_rejected(harness: Harness) -> None:
    try:
        client = harness.authenticated_client()
        response = await client.post(
            "/console/actions/pause/prepare", data={"target": "demo"}
        )
    finally:
        await harness.aclose()

    assert response.status_code == 403
    assert "csrf_rejected" in response.text
    assert harness.executor.calls == []


@pytest.mark.asyncio
async def test_form_post_with_wrong_csrf_is_rejected(harness: Harness) -> None:
    try:
        client = harness.authenticated_client()
        response = await client.post(
            "/console/actions/pause/prepare",
            data={"target": "demo", "csrf": "nonce.not-a-real-mac"},
        )
    finally:
        await harness.aclose()

    assert response.status_code == 403
    assert "csrf_rejected" in response.text


@pytest.mark.asyncio
async def test_csrf_from_another_session_is_rejected(harness: Harness) -> None:
    try:
        first = harness.authenticated_client()
        second = harness.authenticated_client()
        second_form = await get_form(second, "pause")
        stolen = hidden_field(second_form.text, "csrf")
        response = await first.post(
            "/console/actions/pause/prepare",
            data={"target": "demo", "csrf": stolen},
        )
    finally:
        await harness.aclose()

    assert response.status_code == 403
    assert harness.executor.calls == []


@pytest.mark.asyncio
async def test_confirm_post_without_csrf_never_executes(harness: Harness) -> None:
    try:
        client = harness.authenticated_client()
        prepared = await prepare_action(client, "pause", "demo")
        response = await client.post(
            "/console/actions/pause/confirm",
            data={
                "confirmation_id": hidden_field(prepared.text, "confirmation_id"),
                "confirmation_phrase": "PAUSE DEMO",
                "idempotency_key": "k-1",
            },
        )
    finally:
        await harness.aclose()

    assert response.status_code == 403
    assert harness.executor.calls == []


# --- status and queue pages -------------------------------------------------


@pytest.mark.asyncio
async def test_status_page_renders_every_summary_section(harness: Harness) -> None:
    try:
        response = await harness.authenticated_client().get("/console")
    finally:
        await harness.aclose()

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    page = response.text
    for value in (
        "demo",
        "lead-1",
        "max-a",
        "task-1",
        "task-2",
        "green",
        "waiting on review",
    ):
        assert value in page, value


@pytest.mark.asyncio
async def test_status_page_links_every_action_form(harness: Harness) -> None:
    try:
        response = await harness.authenticated_client().get("/console")
    finally:
        await harness.aclose()

    for slug in ACTION_SLUGS:
        assert f'href="/console/actions/{slug}"' in response.text, slug


@pytest.mark.asyncio
async def test_view_values_are_html_escaped(harness: Harness) -> None:
    try:
        harness.status.summary_value = sanitized_summary(
            project_alias="<script>alert(1)</script>"
        )
        response = await harness.authenticated_client().get("/console")
    finally:
        await harness.aclose()

    assert response.status_code == 200
    assert "<script>" not in response.text
    assert "&lt;script&gt;" in response.text


# --- accessibility and mobile rendering -------------------------------------


@pytest.mark.asyncio
async def test_every_page_is_mobile_first_and_accessible(harness: Harness) -> None:
    try:
        client = harness.authenticated_client()
        prepared = await prepare_action(client, "pause", "demo")
        result = await confirm_action(client, "pause", prepared.text, "PAUSE DEMO")
        error = await prepare_action(client, "pause", "ghost")
        pages = [
            await client.get("/console"),
            await client.get("/projects/demo"),
            await harness.anonymous_client().get("/login"),
            await get_form(client, "pause"),
            await get_form(client, "queue"),
            prepared,
            result,
            error,
        ]
    finally:
        await harness.aclose()

    for page in pages:
        html = page.text
        url = page.request.url
        assert VIEWPORT_META in html, url
        assert '<html lang="en">' in html, url
        assert "<main>" in html, url
        assert "<h1>" in html, url
        assert "<script" not in html, url
        # The production CSP has no unsafe-inline: styling must arrive only
        # through the packaged same-origin stylesheet.
        assert '<link rel="stylesheet" href="/static/console.css">' in html, url
        assert "<style" not in html, url
        assert " style=" not in html, url


@pytest.mark.asyncio
async def test_stylesheet_is_served_same_origin_with_the_touch_rules(
    harness: Harness,
) -> None:
    try:
        response = await harness.anonymous_client().get("/static/console.css")
    finally:
        await harness.aclose()

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/css")
    for rule in ("min-height: 44px", "min-width: 44px", "font-size: 16px"):
        assert rule in response.text, rule


@pytest.mark.asyncio
async def test_every_visible_input_has_a_matching_label(harness: Harness) -> None:
    try:
        client = harness.authenticated_client()
        prepared = await prepare_action(
            client, "queue", "demo", MUTATION_ACTIONS[-1][2]
        )
        pages = [
            *[await get_form(client, slug) for slug in ACTION_SLUGS],
            prepared,
        ]
    finally:
        await harness.aclose()

    for page in pages:
        html = page.text
        inputs = re.findall(r"<input[^>]*>", html)
        assert inputs, page.request.url
        for tag in inputs:
            if 'type="hidden"' in tag:
                continue
            identifier = re.search(r'id="([^"]+)"', tag)
            assert identifier is not None, (page.request.url, tag)
            assert f'<label for="{identifier.group(1)}"' in html, (
                page.request.url,
                tag,
            )


@pytest.mark.asyncio
async def test_status_page_has_navigation_landmark(harness: Harness) -> None:
    try:
        response = await harness.authenticated_client().get("/console")
    finally:
        await harness.aclose()

    assert "<nav>" in response.text


@pytest.mark.asyncio
async def test_security_headers_apply_to_console_pages(harness: Harness) -> None:
    try:
        client = harness.authenticated_client()
        responses = [
            await client.get("/console"),
            await client.get("/"),
            await client.get("/projects/demo"),
            await get_form(client, "pause"),
            await harness.anonymous_client().get("/console"),
            await harness.anonymous_client().get("/"),
            await harness.anonymous_client().get("/login"),
            await harness.anonymous_client().get("/static/console.css"),
        ]
    finally:
        await harness.aclose()

    for response in responses:
        assert response.headers.get("cache-control") == "no-store", (
            response.request.url
        )
        assert response.headers.get("x-content-type-options") == "nosniff"
        assert response.headers.get("content-security-policy") == (
            "default-src 'self'; frame-ancestors 'none'; form-action 'self'"
        )


# --- interaction: the full flow for every action ----------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("slug", "target", "extra", "phrase", "intent", "id_field", "id_value"),
    MUTATION_ACTIONS,
)
async def test_each_action_flows_prepare_confirm_execute(
    harness: Harness,
    slug: str,
    target: str,
    extra: dict,
    phrase: str,
    intent: str,
    id_field: str,
    id_value: str,
) -> None:
    try:
        client = harness.authenticated_client()
        prepared = await prepare_action(client, slug, target, extra)
        assert prepared.status_code == 200
        assert phrase in prepared.text
        result = await confirm_action(client, slug, prepared.text, phrase)
    finally:
        await harness.aclose()

    assert result.status_code == 200
    assert "accepted" in result.text
    assert "corr-1" in result.text
    assert len(harness.executor.calls) == 1
    payload = harness.executor.calls[0]
    assert payload["intent"] == intent
    assert payload[id_field] == id_value


@pytest.mark.asyncio
async def test_result_page_never_renders_executor_state_values(
    harness: Harness,
) -> None:
    try:
        client = harness.authenticated_client()
        result = await run_action(client, "pause", "demo", "PAUSE DEMO")
    finally:
        await harness.aclose()

    assert result.status_code == 200
    assert "applied" not in result.text


@pytest.mark.asyncio
async def test_wrong_phrase_is_a_static_conflict_page(harness: Harness) -> None:
    try:
        client = harness.authenticated_client()
        prepared = await prepare_action(client, "pause", "demo")
        response = await confirm_action(client, "pause", prepared.text, "wrong")
    finally:
        await harness.aclose()

    assert response.status_code == 409
    assert "phrase_mismatch" in response.text
    assert harness.executor.calls == []


@pytest.mark.asyncio
async def test_double_submit_of_the_same_confirm_form_executes_once(
    harness: Harness,
) -> None:
    try:
        client = harness.authenticated_client()
        prepared = await prepare_action(client, "pause", "demo")
        first = await confirm_action(client, "pause", prepared.text, "PAUSE DEMO")
        second = await confirm_action(client, "pause", prepared.text, "PAUSE DEMO")
    finally:
        await harness.aclose()

    assert first.status_code == 200
    assert second.status_code == 200
    assert "accepted" in second.text
    assert len(harness.executor.calls) == 1


@pytest.mark.asyncio
async def test_replayed_confirmation_with_new_key_is_rejected(
    harness: Harness,
) -> None:
    try:
        client = harness.authenticated_client()
        prepared = await prepare_action(client, "pause", "demo")
        first = await confirm_action(client, "pause", prepared.text, "PAUSE DEMO")
        replay = await confirm_action(
            client, "pause", prepared.text, "PAUSE DEMO", idempotency_key="other"
        )
    finally:
        await harness.aclose()

    assert first.status_code == 200
    assert replay.status_code == 409
    assert "confirmation_replayed" in replay.text
    assert len(harness.executor.calls) == 1


@pytest.mark.asyncio
async def test_key_reuse_across_confirmations_conflicts(harness: Harness) -> None:
    try:
        client = harness.authenticated_client()
        first_prepared = await prepare_action(client, "pause", "demo")
        key = hidden_field(first_prepared.text, "idempotency_key")
        first = await confirm_action(client, "pause", first_prepared.text, "PAUSE DEMO")
        second_prepared = await prepare_action(client, "resume", "demo")
        second = await confirm_action(
            client, "resume", second_prepared.text, "RESUME DEMO", idempotency_key=key
        )
    finally:
        await harness.aclose()

    assert first.status_code == 200
    assert second.status_code == 409
    assert "idempotency_key_conflict" in second.text
    assert len(harness.executor.calls) == 1


@pytest.mark.asyncio
async def test_expired_confirmation_renders_a_static_gone_page(
    harness: Harness,
) -> None:
    try:
        client = harness.authenticated_client()
        prepared = await prepare_action(client, "pause", "demo")
        harness.clock.advance(seconds=121)
        response = await confirm_action(client, "pause", prepared.text, "PAUSE DEMO")
    finally:
        await harness.aclose()

    assert response.status_code == 410
    assert "confirmation_expired" in response.text
    assert harness.executor.calls == []


@pytest.mark.asyncio
async def test_crash_at_the_mutation_boundary_fails_closed(
    harness: Harness,
) -> None:
    try:
        client = harness.authenticated_client()
        prepared = await prepare_action(client, "pause", "demo")
        harness.executor.crash_next = True
        with pytest.raises(SimulatedCrash):
            await confirm_action(client, "pause", prepared.text, "PAUSE DEMO")
        retry = await confirm_action(client, "pause", prepared.text, "PAUSE DEMO")
    finally:
        await harness.aclose()

    assert retry.status_code == 409
    assert "execution_unresolved" in retry.text
    assert len(harness.executor.calls) == 1


@pytest.mark.asyncio
async def test_another_sessions_confirmation_is_unknown(harness: Harness) -> None:
    try:
        owner = harness.authenticated_client()
        intruder = harness.authenticated_client()
        prepared = await prepare_action(owner, "pause", "demo")
        intruder_form = await get_form(intruder, "pause")
        response = await intruder.post(
            "/console/actions/pause/confirm",
            data={
                "confirmation_id": hidden_field(prepared.text, "confirmation_id"),
                "confirmation_phrase": "PAUSE DEMO",
                "idempotency_key": "k-1",
                "csrf": hidden_field(intruder_form.text, "csrf"),
            },
        )
    finally:
        await harness.aclose()

    assert response.status_code == 404
    assert "unknown_confirmation" in response.text
    assert harness.executor.calls == []


# --- queue instruction identity ---------------------------------------------


@pytest.mark.asyncio
async def test_queue_form_offers_no_operator_instruction_input(
    harness: Harness,
) -> None:
    try:
        response = await get_form(harness.authenticated_client(), "queue")
    finally:
        await harness.aclose()

    assert response.status_code == 200
    assert 'name="issue_id"' in response.text
    assert 'name="priority"' in response.text
    assert "operator_instruction" not in response.text


@pytest.mark.asyncio
async def test_queue_executes_with_a_generated_session_bound_instruction_id(
    harness: Harness,
) -> None:
    try:
        client = harness.authenticated_client()
        prepared = await prepare_action(
            client, "queue", "demo", {"issue_id": "task-9", "priority": "2"}
        )
        result = await confirm_action(
            client, "queue", prepared.text, "QUEUE_ISSUE DEMO"
        )
    finally:
        await harness.aclose()

    assert result.status_code == 200
    assert len(harness.executor.calls) == 1
    instruction_id = harness.executor.calls[0]["operator_instruction_id"]
    assert instruction_id
    assert client_session_id(client) in instruction_id
    # The generated identity is audit data, never page content.
    assert instruction_id not in prepared.text
    assert instruction_id not in result.text


@pytest.mark.asyncio
async def test_injected_operator_instruction_id_never_reaches_the_executor(
    harness: Harness,
) -> None:
    try:
        response = await prepare_action(
            harness.authenticated_client(),
            "queue",
            "demo",
            {
                "issue_id": "task-9",
                "priority": "2",
                "operator_instruction_id": "chat-7",
            },
        )
    finally:
        await harness.aclose()

    assert response.status_code == 400
    assert "invalid_request" in response.text
    assert "chat-7" not in response.text
    assert harness.executor.calls == []


@pytest.mark.asyncio
async def test_distinct_submissions_receive_distinct_instruction_ids(
    harness: Harness,
) -> None:
    try:
        client = harness.authenticated_client()
        for _ in range(2):
            result = await run_action(
                client,
                "queue",
                "demo",
                "QUEUE_ISSUE DEMO",
                {"issue_id": "task-9", "priority": "2"},
            )
            assert result.status_code == 200
    finally:
        await harness.aclose()

    assert len(harness.executor.calls) == 2
    first, second = (
        call["operator_instruction_id"] for call in harness.executor.calls
    )
    assert first and second
    assert first != second


@pytest.mark.asyncio
async def test_idempotent_retry_keeps_the_original_instruction_identity(
    harness: Harness,
) -> None:
    try:
        client = harness.authenticated_client()
        prepared = await prepare_action(
            client, "queue", "demo", {"issue_id": "task-9", "priority": "2"}
        )
        first = await confirm_action(
            client, "queue", prepared.text, "QUEUE_ISSUE DEMO"
        )
        retry = await confirm_action(
            client, "queue", prepared.text, "QUEUE_ISSUE DEMO"
        )
    finally:
        await harness.aclose()

    assert first.status_code == 200
    assert retry.status_code == 200
    assert "accepted" in retry.text
    # One prepare means one minted identity: the retry returns the stored
    # result instead of executing a second command.
    assert len(harness.executor.calls) == 1
    assert harness.executor.calls[0]["operator_instruction_id"]


# --- root and project pages -------------------------------------------------


def two_project_summary() -> OperationsSummary:
    """Rows for 'demo' plus a second project whose values act as markers."""

    base = sanitized_summary()
    return OperationsSummary(
        projects=(
            *base.projects,
            ProjectView(project_alias="orbit", state="paused", age_seconds=8),
        ),
        workers=(
            *base.workers,
            WorkerView(
                worker_alias="lead-9",
                profile_alias="max-b",
                project_alias="orbit",
                state="working",
                age_seconds=45,
                context_band=ContextBand.OK,
            ),
        ),
        queue=(
            *base.queue,
            QueueItemView(
                issue_alias="task-77",
                project_alias="orbit",
                state="queued",
                age_seconds=6,
            ),
        ),
        resources=base.resources,
        reviews=(
            *base.reviews,
            ReviewView(
                issue_alias="task-88",
                project_alias="orbit",
                state="in_review",
                pr_url=MARKER_PR_URL,
                ci_status="amber",
                age_seconds=70,
            ),
        ),
        stalls=(
            *base.stalls,
            StallView(
                worker_alias="lead-9",
                project_alias="orbit",
                stall_summary="waiting on ci",
                age_seconds=300,
                checkpoint_eligible=False,
            ),
        ),
    )


@pytest.mark.asyncio
async def test_root_sends_anonymous_visitors_to_the_login_flow(
    harness: Harness,
) -> None:
    try:
        client = harness.anonymous_client()
        response = await client.get("/")
        login_page = await client.get("/login")
    finally:
        await harness.aclose()

    assert response.status_code == 303
    assert response.headers["location"] == "/login"
    assert login_page.status_code == 200


@pytest.mark.asyncio
async def test_root_sends_authenticated_operators_to_the_dashboard(
    harness: Harness,
) -> None:
    try:
        client = harness.authenticated_client()
        response = await client.get("/")
        assert response.status_code == 303
        dashboard = await client.get(response.headers["location"])
    finally:
        await harness.aclose()

    assert response.headers["location"] == "/console"
    assert dashboard.status_code == 200
    assert "<nav>" in dashboard.text


@pytest.mark.asyncio
async def test_project_page_requires_authentication(harness: Harness) -> None:
    try:
        response = await harness.anonymous_client().get("/projects/demo")
    finally:
        await harness.aclose()

    assert response.status_code == 401
    assert "unauthorized" in response.text


@pytest.mark.asyncio
async def test_project_page_renders_only_that_projects_rows(
    harness: Harness,
) -> None:
    try:
        harness.status.summary_value = two_project_summary()
        response = await harness.authenticated_client().get("/projects/demo")
    finally:
        await harness.aclose()

    assert response.status_code == 200
    page = response.text
    for value in (
        "demo",
        "lead-1",
        "task-1",
        "task-2",
        CANONICAL_PR_URL,
        "waiting on review",
    ):
        assert value in page, value
    for marker in (
        "orbit",
        "lead-9",
        "task-77",
        "task-88",
        MARKER_PR_URL,
        "waiting on ci",
    ):
        assert marker not in page, marker


@pytest.mark.asyncio
async def test_second_project_page_renders_only_its_own_rows(
    harness: Harness,
) -> None:
    try:
        harness.status.summary_value = two_project_summary()
        response = await harness.authenticated_client().get("/projects/orbit")
    finally:
        await harness.aclose()

    assert response.status_code == 200
    page = response.text
    for value in (
        "orbit",
        "lead-9",
        "task-77",
        "task-88",
        MARKER_PR_URL,
        "waiting on ci",
    ):
        assert value in page, value
    for marker in (
        "lead-1",
        "task-1",
        "task-2",
        CANONICAL_PR_URL,
        "waiting on review",
    ):
        assert marker not in page, marker


@pytest.mark.asyncio
async def test_unknown_project_is_a_static_not_found_without_echo(
    harness: Harness,
) -> None:
    try:
        client = harness.authenticated_client()
        plain = await client.get("/projects/ghost-hunter2")
        hostile = await client.get("/projects/%3Cscript%3Ealert(1)%3C-script%3E")
    finally:
        await harness.aclose()

    for response in (plain, hostile):
        assert response.status_code == 404
        assert "unknown_project" in response.text
    assert "ghost" not in plain.text
    assert "hunter2" not in plain.text
    assert "<script" not in hostile.text
    assert "alert" not in hostile.text


@pytest.mark.asyncio
async def test_dashboard_and_project_pages_link_projects_and_prs_only(
    harness: Harness,
) -> None:
    try:
        harness.status.summary_value = two_project_summary()
        client = harness.authenticated_client()
        dashboard = await client.get("/console")
        demo = await client.get("/projects/demo")
        orbit = await client.get("/projects/orbit")
    finally:
        await harness.aclose()

    assert 'href="/projects/demo"' in dashboard.text
    assert 'href="/projects/orbit"' in dashboard.text
    internal = {
        "/",
        "/console",
        "/projects/demo",
        "/projects/orbit",
        "/static/console.css",
    } | {f"/console/actions/{slug}" for slug in ACTION_SLUGS}
    # Each page may carry exactly its own projects' canonical PR links: the
    # dashboard both, each project page only its own repository's URL.
    expected_external = (
        (dashboard, {CANONICAL_PR_URL, MARKER_PR_URL}),
        (demo, {CANONICAL_PR_URL}),
        (orbit, {MARKER_PR_URL}),
    )
    for page, external in expected_external:
        assert page.status_code == 200
        hrefs = re.findall(r'href="([^"]+)"', page.text)
        assert {
            href for href in hrefs if not href.startswith("/")
        } == external, page.request.url
        for href in hrefs:
            assert href in internal | external, (page.request.url, href)
        assert "admin" not in page.text.lower()


class _PoisonedReviewSource:
    """A StatusSource whose only review carries an unprovable link binding."""

    def __init__(self, pr_url: str, project_alias: str = "demo") -> None:
        self._pr_url = pr_url
        self._project_alias = project_alias

    def projects(self):
        return []

    def workers(self):
        return []

    def queue_items(self):
        return []

    def resources(self):
        return None

    def reviews(self):
        return [
            ReviewInput(
                issue_alias="task-2",
                project_alias=self._project_alias,
                state="in_review",
                pr_url=self._pr_url,
                ci_status="green",
                age_seconds=90,
            )
        ]

    def stalls(self):
        return []


class _ListAuditSink:
    def __init__(self) -> None:
        self.events = []

    def append(self, event) -> None:
        self.events.append(event)


@pytest.mark.asyncio
async def test_unverified_review_link_fails_closed_without_leaking(
    harness: Harness,
) -> None:
    """An unprovable pr_url aborts the real sanitized view, so the console
    serves the static redaction page and the value reaches no response."""

    hostile = "https://phishing.example/pull/7"
    sink = _ListAuditSink()
    harness.status.provider = StatusViewService(
        source=_PoisonedReviewSource(hostile),
        audit=sink,
        github_repos=CONFIGURED_GITHUB_REPOS,
    )
    try:
        client = harness.authenticated_client()
        dashboard = await client.get("/console")
        project = await client.get("/projects/demo")
    finally:
        await harness.aclose()

    for response in (dashboard, project):
        assert response.status_code == 500
        assert "redaction_failure" in response.text
        assert hostile not in response.text
        assert "phishing" not in response.text
    assert len(sink.events) == 2
    for event in sink.events:
        assert event.payload["field"] == "pr_url"
        assert hostile not in json.dumps(event.payload)


@pytest.mark.parametrize(
    ("project_alias", "pr_url"),
    [
        # Project B carrying project A's canonical URL: canonical somewhere
        # is not canonical HERE — the binding to the row's project must hold.
        ("orbit", CANONICAL_PR_URL),
        ("demo", MARKER_PR_URL),
        # An unmapped alias fails closed even with a canonical-looking URL.
        ("ghost-project", CANONICAL_PR_URL),
    ],
)
@pytest.mark.asyncio
async def test_misbound_review_link_fails_closed_without_leaking(
    harness: Harness, project_alias: str, pr_url: str
) -> None:
    """A pr_url not provably bound to its own project's repository aborts
    the sanitized view: the static redaction page is served and neither the
    URL nor the alias reaches a response body or an audit payload."""

    sink = _ListAuditSink()
    harness.status.provider = StatusViewService(
        source=_PoisonedReviewSource(pr_url, project_alias),
        audit=sink,
        github_repos=CONFIGURED_GITHUB_REPOS,
    )
    try:
        client = harness.authenticated_client()
        dashboard = await client.get("/console")
        project = await client.get(f"/projects/{project_alias}")
    finally:
        await harness.aclose()

    for response in (dashboard, project):
        assert response.status_code == 500
        assert "redaction_failure" in response.text
        assert pr_url not in response.text
        assert project_alias not in response.text
    assert len(sink.events) == 2
    for event in sink.events:
        assert event.payload["field"] == "pr_url"
        serialized = json.dumps(event.payload)
        assert pr_url not in serialized
        assert project_alias not in serialized


# --- validation and static codes --------------------------------------------


@pytest.mark.asyncio
async def test_unknown_action_is_a_static_not_found(harness: Harness) -> None:
    try:
        client = harness.authenticated_client()
        page = await client.get("/console/actions/frobnicate")
        form = await get_form(client, "pause")
        posted = await client.post(
            "/console/actions/frobnicate/prepare",
            data={"target": "demo", "csrf": hidden_field(form.text, "csrf")},
        )
    finally:
        await harness.aclose()

    assert page.status_code == 404
    assert "unknown_action" in page.text
    assert posted.status_code == 404
    assert "frobnicate" not in page.text


@pytest.mark.asyncio
async def test_unknown_target_is_a_static_not_found(harness: Harness) -> None:
    try:
        client = harness.authenticated_client()
        response = await prepare_action(client, "pause", "ghost")
    finally:
        await harness.aclose()

    assert response.status_code == 404
    assert "unknown_target" in response.text
    assert "ghost" not in response.text


@pytest.mark.asyncio
async def test_invalid_parameters_are_a_static_code(harness: Harness) -> None:
    try:
        client = harness.authenticated_client()
        response = await prepare_action(
            client,
            "queue",
            "demo",
            {"issue_id": "hunter2-issue", "priority": "9"},
        )
    finally:
        await harness.aclose()

    assert response.status_code == 400
    assert "invalid_parameters" in response.text
    assert "hunter2" not in response.text
    assert harness.executor.calls == []


@pytest.mark.asyncio
async def test_extra_form_fields_are_rejected_without_echo(
    harness: Harness,
) -> None:
    try:
        client = harness.authenticated_client()
        form = await get_form(client, "pause")
        response = await client.post(
            "/console/actions/pause/prepare",
            data={
                "target": "demo",
                "csrf": hidden_field(form.text, "csrf"),
                "sneaky": "hunter2-value",
            },
        )
    finally:
        await harness.aclose()

    assert response.status_code == 400
    assert "invalid_request" in response.text
    assert "hunter2" not in response.text
    assert harness.executor.calls == []


@pytest.mark.asyncio
async def test_missing_form_fields_are_rejected(harness: Harness) -> None:
    try:
        client = harness.authenticated_client()
        form = await get_form(client, "pause")
        response = await client.post(
            "/console/actions/pause/prepare",
            data={"csrf": hidden_field(form.text, "csrf")},
        )
    finally:
        await harness.aclose()

    assert response.status_code == 400
    assert "invalid_request" in response.text


# --- leakage sweep ----------------------------------------------------------


@pytest.mark.asyncio
async def test_no_forbidden_term_appears_in_any_console_page(
    harness: Harness,
) -> None:
    try:
        client = harness.authenticated_client()
        anonymous = harness.anonymous_client()
        prepared = await prepare_action(client, "pause", "demo")
        responses = [
            await anonymous.get("/console"),
            await anonymous.get("/"),
            await anonymous.get("/static/console.css"),
            await client.get("/console"),
            await client.get("/"),
            await client.get("/projects/demo"),
            await client.get("/projects/ghost"),
            *[await get_form(client, slug) for slug in ACTION_SLUGS],
            prepared,
            await confirm_action(client, "pause", prepared.text, "wrong"),
            await confirm_action(client, "pause", prepared.text, "PAUSE DEMO"),
            await prepare_action(client, "pause", "ghost"),
            await client.get("/console/actions/frobnicate"),
            await client.post(
                "/console/actions/pause/prepare", data={"target": "demo"}
            ),
        ]
    finally:
        await harness.aclose()

    for response in responses:
        lowered = sweepable_text(response)
        for term in FORBIDDEN_VALUE_TERMS:
            assert term not in lowered, (response.request.url, term)
