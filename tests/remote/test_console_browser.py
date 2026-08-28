"""Real-browser rendering tests under the production CSP (INFRA-176 rework).

Correction packet 3 mandates verifying the actual rendered layout in a
browser, so this module — alone in the remote suite — starts uvicorn on an
ephemeral loopback port and drives headless Chromium against it. The
loopback-only boundary is preserved, everything here is test-only, and no
flow in this module touches the sqlite database, so the same-thread ASGI
harness rules stay intact for every other test module.
"""

import threading
import time

import httpx
import pytest
import uvicorn
from playwright.sync_api import sync_playwright

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.remote.api import (
    RemoteDependencies,
    create_operations_app,
)
from hermes_orchestrator.remote.auth import (
    REMOTE_SERVICE,
    TOKEN_ACCOUNT,
    CsrfService,
    LoginRateLimiter,
    RemoteCredentialService,
    SessionService,
)
from hermes_orchestrator.remote.commands import ConfirmationService
from hermes_orchestrator.remote.policy import RemotePolicy
from tests.remote.test_auth import FakeKeychain
from tests.remote.test_console import (
    CANONICAL_PR_URL,
    MARKER_PR_URL,
    FakeExecutor,
    FakeStatus,
    FakeTargets,
    two_project_summary,
)

APP_TOKEN = "application-value-for-browser-tests"
VIEWPORTS = ((390, 844), (430, 932))
CSP_VALUE = "default-src 'self'; frame-ancestors 'none'; form-action 'self'"

TOUCH_TARGET_SIZES = """
() => Array.from(
    document.querySelectorAll('nav a, button, input:not([type="hidden"])')
).map((element) => {
    const box = element.getBoundingClientRect();
    return [element.tagName, box.width, box.height];
})
"""

CSP_VIOLATION_LISTENER = """
window.__cspViolations = [];
document.addEventListener('securitypolicyviolation', (event) => {
    window.__cspViolations.push(
        event.violatedDirective + ' ' + event.blockedURI
    );
});
"""


@pytest.fixture(scope="module")
def server_url(tmp_path_factory: pytest.TempPathFactory):
    keychain = FakeKeychain()
    keychain.create(REMOTE_SERVICE, "session-signing", "signing-secret-for-tests")
    keychain.create(REMOTE_SERVICE, TOKEN_ACCOUNT, APP_TOKEN)
    database_path = tmp_path_factory.mktemp("browser") / "state.db"
    holder: dict[str, uvicorn.Server] = {}
    configured = threading.Event()

    def serve() -> None:
        # The sqlite connection is single-thread (check_same_thread), and
        # the confirm flow now writes from the serving thread — so the
        # database is opened, used, and closed entirely inside it.
        database = Database.open(database_path)
        try:
            # Two projects with reviews in DIFFERENT configured repositories,
            # so the rendered pages must bind each link to its own project.
            status = FakeStatus()
            status.summary_value = two_project_summary()
            dependencies = RemoteDependencies(
                sessions=SessionService(keychain=keychain),
                csrf=CsrfService(keychain=keychain),
                policy=RemotePolicy(),
                status=status,
                confirmations=ConfirmationService(
                    database=database,
                    events=EventStore(database),
                    policy=RemotePolicy(),
                    targets=FakeTargets(),
                    executor=FakeExecutor(),
                ),
                credentials=RemoteCredentialService(keychain=keychain),
                login_limiter=LoginRateLimiter(),
            )
            config = uvicorn.Config(
                create_operations_app(dependencies),
                host="127.0.0.1",
                port=0,
                log_level="warning",
            )
            holder["server"] = uvicorn.Server(config)
            configured.set()
            holder["server"].run()
        finally:
            database.close()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    assert configured.wait(timeout=15), "server thread never configured"
    server = holder["server"]
    deadline = time.monotonic() + 15
    while not server.started:
        if time.monotonic() > deadline:
            raise RuntimeError("uvicorn did not start inside the deadline")
        time.sleep(0.05)
    port = server.servers[0].sockets[0].getsockname()[1]
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=15)


def browser_login(page, server_url: str) -> None:
    response = page.goto(f"{server_url}/login")
    assert response is not None and response.ok
    page.fill("#passcode", APP_TOKEN)
    page.click("button[type=submit]")
    page.wait_for_url(f"{server_url}/console")


def assert_rendered_cleanly(page, origin: str, width: int) -> None:
    """The page must be styled, single-column, touchable, and inline-free."""

    assert page.evaluate("() => window.__cspViolations") == []
    assert page.evaluate("() => document.styleSheets.length") == 1
    assert page.eval_on_selector(
        "button", "(element) => getComputedStyle(element).minHeight"
    ) == "44px"
    assert page.evaluate("() => document.documentElement.scrollWidth") <= width
    assert page.evaluate("() => document.body.scrollWidth") <= width
    for tag, box_width, box_height in page.evaluate(TOUCH_TARGET_SIZES):
        assert box_width >= 44, (tag, box_width)
        assert box_height >= 44, (tag, box_height)
    content = page.content()
    assert "<style" not in content
    assert " style=" not in content
    assert "<script" not in content


@pytest.mark.parametrize(("width", "height"), VIEWPORTS)
def test_console_renders_cleanly_at_phone_viewports(
    server_url: str, width: int, height: int
) -> None:
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        try:
            context = browser.new_context(
                viewport={"width": width, "height": height}
            )
            page = context.new_page()
            page.add_init_script(CSP_VIOLATION_LISTENER)
            console_errors: list[str] = []
            failed_requests: list[str] = []
            request_urls: list[str] = []
            page.on(
                "console",
                lambda message: console_errors.append(message.text)
                if message.type == "error"
                else None,
            )
            page.on(
                "requestfailed",
                lambda request: failed_requests.append(request.url),
            )
            page.on("request", lambda request: request_urls.append(request.url))

            stylesheet = f"{server_url}/static/console.css"
            login = page.goto(f"{server_url}/login")
            assert login is not None and login.ok
            assert_rendered_cleanly(page, server_url, width)

            page.fill("#passcode", APP_TOKEN)
            page.click("button[type=submit]")
            page.wait_for_url(f"{server_url}/console")
            # An authenticated render, not the static unauthorized page: the
            # hardened cookie survived the loopback origin.
            page.wait_for_selector("nav")
            assert "Pause project" in page.content()
            assert_rendered_cleanly(page, server_url, width)

            assert stylesheet in request_urls
            assert failed_requests == []
            assert console_errors == []
            # Only same-origin assets: no third-party requests, no remote
            # fonts, nothing outside the loopback origin.
            for url in request_urls:
                assert url.startswith(server_url), url
        finally:
            browser.close()


def test_login_field_masks_the_token_and_still_authenticates(
    server_url: str,
) -> None:
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        try:
            context = browser.new_context(viewport={"width": 390, "height": 844})
            page = context.new_page()
            console_messages: list[str] = []
            request_urls: list[str] = []
            page.on("console", lambda message: console_messages.append(message.text))
            page.on("request", lambda request: request_urls.append(request.url))

            response = page.goto(f"{server_url}/login")
            assert response is not None and response.ok
            assert (
                page.eval_on_selector("#passcode", "(el) => el.type") == "password"
            )
            assert (
                page.eval_on_selector("#passcode", "(el) => el.autocomplete")
                == "current-password"
            )
            page.fill("#passcode", APP_TOKEN)
            # The typed token exists only as the control's value; it never
            # reaches the serialized document, a URL, or the console.
            assert APP_TOKEN not in page.content()
            assert (
                page.eval_on_selector("#passcode", "(el) => el.value") == APP_TOKEN
            )
            page.click("button[type=submit]")
            page.wait_for_url(f"{server_url}/console")
            page.wait_for_selector("nav")
            assert APP_TOKEN not in page.content()
            for url in request_urls:
                assert APP_TOKEN not in url, url
            for message in console_messages:
                assert APP_TOKEN not in message, message
        finally:
            browser.close()


RESOLVED_ANCHOR_HREFS = """
() => Array.from(document.querySelectorAll('a')).map((el) => el.href)
"""


def test_review_links_resolve_to_each_projects_configured_pull_request(
    server_url: str,
) -> None:
    """The only off-origin destinations the console offers are the exact
    configured GitHub pull request URLs, each bound to its own project: the
    dashboard carries both projects' links, each project page only its own —
    asserted on the anchors' resolved ``href`` properties without ever
    navigating off the loopback origin."""

    expectations = (
        ("/console", [CANONICAL_PR_URL, MARKER_PR_URL]),
        ("/projects/demo", [CANONICAL_PR_URL]),
        ("/projects/orbit", [MARKER_PR_URL]),
    )
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        try:
            context = browser.new_context(viewport={"width": 390, "height": 844})
            page = context.new_page()
            request_urls: list[str] = []
            page.on("request", lambda request: request_urls.append(request.url))
            browser_login(page, server_url)
            for path, expected in expectations:
                response = page.goto(f"{server_url}{path}")
                assert response is not None and response.ok
                hrefs = page.evaluate(RESOLVED_ANCHOR_HREFS)
                external = [
                    href for href in hrefs if not href.startswith(server_url)
                ]
                assert sorted(external) == sorted(expected), (path, external)
            # The links are inspected, never followed: every request stays
            # on the loopback origin.
            for url in request_urls:
                assert url.startswith(server_url), url
        finally:
            browser.close()


STICKY_BOX = """
() => {
    const box = document
        .querySelector('form.confirm-controls')
        .getBoundingClientRect();
    return [box.top, box.bottom, box.height];
}
"""

INSERT_FILLER = """
() => {
    const filler = document.createElement('div');
    filler.style.height = '3000px';
    const form = document.querySelector('form.confirm-controls');
    form.parentNode.insertBefore(filler, form);
}
"""


@pytest.mark.parametrize(("width", "height"), VIEWPORTS)
def test_confirmation_controls_stay_visible_while_scrolling(
    server_url: str, width: int, height: int
) -> None:
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        try:
            context = browser.new_context(
                viewport={"width": width, "height": height}
            )
            page = context.new_page()
            browser_login(page, server_url)
            page.click('a[href="/console/actions/pause"]')
            page.wait_for_selector("#target")
            page.fill("#target", "demo")
            page.click("button[type=submit]")
            page.wait_for_selector("form.confirm-controls")
            assert "PAUSE DEMO" in page.content()
            assert (
                page.eval_on_selector(
                    "form.confirm-controls",
                    "(el) => getComputedStyle(el).position",
                )
                == "sticky"
            )
            # Force real overflow ahead of the controls (a CSSOM height
            # assignment — no style attribute enters the markup, so the CSP
            # posture under test is unchanged), then check the sticky form
            # keeps its whole box inside the viewport at both extremes.
            page.evaluate(INSERT_FILLER)
            assert (
                page.evaluate("() => document.documentElement.scrollHeight")
                > height
            )
            for scroll in (
                "() => window.scrollTo(0, 0)",
                "() => window.scrollTo(0, document.documentElement.scrollHeight)",
            ):
                page.evaluate(scroll)
                top, bottom, box_height = page.evaluate(STICKY_BOX)
                assert box_height > 0
                assert top >= 0, (scroll, top, bottom)
                assert bottom <= height, (scroll, top, bottom)
        finally:
            browser.close()


def test_html_and_stylesheet_keep_the_security_headers(server_url: str) -> None:
    with httpx.Client(base_url=server_url) as client:
        for path, content_type in (
            ("/login", "text/html"),
            ("/static/console.css", "text/css"),
        ):
            response = client.get(path)
            assert response.status_code == 200, path
            assert response.headers["content-type"].startswith(content_type)
            assert response.headers["cache-control"] == "no-store"
            assert response.headers["content-security-policy"] == CSP_VALUE
            assert response.headers["x-content-type-options"] == "nosniff"
            assert response.headers["referrer-policy"] == "no-referrer"
