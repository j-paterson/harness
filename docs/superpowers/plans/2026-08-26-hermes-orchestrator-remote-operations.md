# Restricted remote operations implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Provide a tailnet-only, independently authenticated iPhone operations console and a narrow Photon/iMessage command channel suitable for Apple Watch notifications without exposing the full Hermes dashboard, credentials, or general-purpose execution.

**Architecture:** A separate FastAPI service reads sanitized orchestration views and submits strictly typed commands. Tailscale Serve terminates tailnet-only HTTPS in front of a loopback listener, while an application token and short-lived session cookie provide a second boundary. Photon maps allowlisted low-sensitivity messages to an even smaller command set.

**Tech Stack:** Prior phases, FastAPI 0.133.1, Uvicorn 0.41.0, Jinja2 3.1.6, python-multipart 0.0.32, stdlib HMAC and secrets, macOS Keychain, Tailscale 1.98.5 Serve, Hermes Agent 0.20.5 Photon sidecar, pytest and HTTPX ASGI tests

**Spec:** docs/superpowers/specs/2026-08-26-hermes-orchestration-system-design.md

## Global constraints

- Execute all four earlier implementation plans and pass their exit gates first.
- The full Hermes dashboard remains bound to 127.0.0.1 and is never a Tailscale Serve target.
- The operations service binds only to 127.0.0.1:8787.
- Use Tailscale Serve only; never enable Tailscale Funnel.
- Tailnet access is necessary but not sufficient; application authentication is mandatory.
- Remote surfaces never expose credentials, raw environment variables, unrestricted shell, MCP administration, account identity, or destructive overrides.
- Every mutating phone action requires a short-lived confirmation and an idempotency key.
- Photon accepts only the operator's allowlisted one-to-one conversation and rejects groups.
- Photon telemetry remains off.
- Photon messages contain only low-sensitivity operational information.
- Mutating features are enabled individually after their security tests pass.
- Every task follows test-driven development and ends with a focused commit.

## File map

- src/hermes_orchestrator/remote/auth.py: Keychain-backed application token, sessions, CSRF, and rate limits.
- src/hermes_orchestrator/remote/policy.py: role and command authorization.
- src/hermes_orchestrator/remote/api.py: status, queue, confirmation, and command endpoints.
- src/hermes_orchestrator/remote/views.py: sanitized read models.
- src/hermes_orchestrator/remote/templates/: mobile HTML templates.
- src/hermes_orchestrator/remote/static/app.css: responsive styles.
- src/hermes_orchestrator/remote/photon.py: sender validation, command grammar, and notification rendering.
- deploy/launchd/com.josystem.hermes-orchestrator.plist: supervisor service.
- deploy/launchd/com.josystem.hermes-operations.plist: loopback operations service.
- scripts/install-launchd.sh: explicit, non-destructive service installation.
- docs/operations/remote-access.md: setup, verification, disable, and recovery runbook.
- tests/: authentication, authorization, command, Photon, and end-to-end security tests.

---

### Task 1: Add independent application authentication

**Files:**
- Modify: pyproject.toml
- Create: src/hermes_orchestrator/remote/__init__.py
- Create: src/hermes_orchestrator/remote/auth.py
- Modify: src/hermes_orchestrator/keychain.py
- Test: tests/remote/test_auth.py

**Interfaces:**
- Produces: RemoteCredentialService.initialize() -> str
- Produces: RemoteCredentialService.verify(token: str) -> bool
- Produces: SessionService.create(client_fingerprint: str) -> SessionCookie
- Produces: SessionService.verify(cookie: str, client_fingerprint: str) -> SessionClaims
- Produces: CsrfService.issue(session_id: str) -> str
- Produces: CsrfService.verify(session_id: str, token: str) -> bool

- [ ] **Step 1: Write failing token, cookie, and CSRF tests**

    def test_initialize_returns_token_once(credentials):
        token = credentials.initialize()
        assert len(token) >= 43
        assert credentials.keychain.read("hermes-orchestrator-remote", "token") == token
        with pytest.raises(CredentialExists):
            credentials.initialize()

    def test_wrong_token_is_constant_time_rejected(credentials, monkeypatch):
        called = False
        def compared(left, right):
            nonlocal called
            called = True
            return False
        monkeypatch.setattr(hmac, "compare_digest", compared)
        assert credentials.verify("wrong") is False
        assert called is True

    def test_session_is_bound_and_expires(session_service, clock):
        cookie = session_service.create("phone-fingerprint")
        assert session_service.verify(cookie.value, "phone-fingerprint").authenticated
        clock.advance(minutes=16)
        with pytest.raises(SessionExpired):
            session_service.verify(cookie.value, "phone-fingerprint")

    def test_csrf_cannot_cross_sessions(csrf):
        token = csrf.issue("session-a")
        assert csrf.verify("session-a", token)
        assert not csrf.verify("session-b", token)

- [ ] **Step 2: Run tests and verify they fail**

Run: uv run pytest tests/remote/test_auth.py -v

Expected: FAIL because remote authentication is undefined.

- [ ] **Step 3: Implement token and session cryptography**

Add fastapi==0.133.1, uvicorn==0.41.0, Jinja2==3.1.6, and python-multipart==0.0.32. Generate 32 random bytes with secrets.token_urlsafe(32), store the token only in Keychain service hermes-orchestrator-remote account token, and display it once from remote-auth-init.

Sign compact JSON session claims with HMAC-SHA256 using a separate 32-byte Keychain secret. Claims contain session_id, issued_at, expires_at=issued_at+900 seconds, and a SHA-256 client fingerprint. Use HttpOnly, Secure, SameSite=Strict, Path=/ cookies. Derive CSRF tokens from session id, method, path, and a per-form nonce. Limit failed login attempts to five per 15 minutes per address and fingerprint.

- [ ] **Step 4: Run authentication tests**

Run: uv run pytest tests/remote/test_auth.py -v

Expected: all tests pass, including tampered, expired, replayed, wrong-fingerprint, and rate-limit cases.

- [ ] **Step 5: Commit**

    git add pyproject.toml uv.lock src/hermes_orchestrator/remote src/hermes_orchestrator/keychain.py tests/remote/test_auth.py
    git commit -m "feat: authenticate the restricted operations console"

### Task 2: Define sanitized views and remote authorization

**Files:**
- Create: src/hermes_orchestrator/remote/policy.py
- Create: src/hermes_orchestrator/remote/views.py
- Test: tests/remote/test_policy.py
- Test: tests/remote/test_views.py

**Interfaces:**
- Produces: RemotePolicy.authorize(intent: RemoteIntent) -> AuthorizationDecision
- Produces: StatusViewService.summary() -> OperationsSummary
- Produces: ProjectView, WorkerView, QueueItemView, ResourceView, ReviewView, StallView

- [ ] **Step 1: Write failing allowlist and redaction tests**

    @pytest.mark.parametrize("intent", [
        "status", "queue_issue", "pause", "resume", "retry", "reprioritize",
        "approve_stall", "approve_handoff", "request_checkpoint", "request_cleanup",
    ])
    def test_allowed_intents(policy, intent):
        assert policy.authorize(RemoteIntent(intent)).allowed

    @pytest.mark.parametrize("intent", [
        "shell", "read_env", "credentials", "mcp_admin", "provider_auth",
        "spend", "force_delete", "disable_safety",
    ])
    def test_forbidden_intents(policy, intent):
        assert not policy.authorize(RemoteIntent(intent)).allowed

    def test_status_view_has_no_identity_or_raw_output(view_service):
        payload = view_service.summary().model_dump()
        serialized = json.dumps(payload)
        for forbidden in ("email", "phone", "token", "secret", "raw_output", "environment"):
            assert forbidden not in serialized.lower()

- [ ] **Step 2: Run tests and verify they fail**

Run: uv run pytest tests/remote/test_policy.py tests/remote/test_views.py -v

Expected: FAIL because policy and views are undefined.

- [ ] **Step 3: Implement explicit read models and deny-by-default policy**

Define RemoteIntent as a closed enum. Unknown values fail parsing. Status models include only opaque project, issue, worker, and profile aliases; state; age; context band; resource measurements; PR URL; known CI status; stall summary; and checkpoint eligibility.

Do not serialize database rows directly. Construct every read model field explicitly. Reject values containing known secret field names before rendering and emit a redaction_failure audit event.

- [ ] **Step 4: Run policy tests**

Run: uv run pytest tests/remote/test_policy.py tests/remote/test_views.py -v

Expected: all tests pass.

- [ ] **Step 5: Commit**

    git add src/hermes_orchestrator/remote/policy.py src/hermes_orchestrator/remote/views.py tests/remote/test_policy.py tests/remote/test_views.py
    git commit -m "feat: constrain remote orchestration views"

### Task 3: Build authenticated read and confirmed mutation endpoints

**Files:**
- Create: src/hermes_orchestrator/remote/api.py
- Create: src/hermes_orchestrator/remote/commands.py
- Test: tests/remote/test_api.py
- Test: tests/remote/test_commands.py

**Interfaces:**
- Produces: create_operations_app(dependencies: RemoteDependencies) -> FastAPI
- Produces: ConfirmationService.prepare(command: RemoteCommand) -> PendingConfirmation
- Produces: ConfirmationService.confirm(id: str, phrase: str, idempotency_key: str) -> CommandResult

- [ ] **Step 1: Write failing endpoint and confirmation tests**

    def test_status_requires_authentication(client):
        assert client.get("/api/status").status_code == 401

    def test_authenticated_status_is_no_store(authenticated_client):
        response = authenticated_client.get("/api/status")
        assert response.status_code == 200
        assert response.headers["cache-control"] == "no-store"

    def test_mutation_requires_csrf_and_confirmation(authenticated_client):
        prepared = authenticated_client.post(
            "/api/commands/prepare",
            json={"intent": "pause", "target": "project:demo"},
            headers={"x-csrf-token": authenticated_client.csrf},
        )
        assert prepared.status_code == 200
        pending = prepared.json()
        assert pending["confirmation_phrase"] == "PAUSE DEMO"
        denied = authenticated_client.post(
            "/api/commands/confirm",
            json={
                "confirmation_id": pending["confirmation_id"],
                "confirmation_phrase": "wrong",
                "idempotency_key": "phone-command-1",
            },
            headers={"x-csrf-token": authenticated_client.csrf},
        )
        assert denied.status_code == 409

    def test_same_idempotency_key_executes_once(authenticated_client):
        first = confirm_pause(authenticated_client, "same-key")
        second = confirm_pause(authenticated_client, "same-key")
        assert first.json() == second.json()
        assert authenticated_client.orchestrator.pause_calls == 1

- [ ] **Step 2: Run tests and verify they fail**

Run: uv run pytest tests/remote/test_api.py tests/remote/test_commands.py -v

Expected: FAIL because the operations API is undefined.

- [ ] **Step 3: Implement loopback-only API behavior**

At startup, reject any configured bind host other than 127.0.0.1. Add security headers: Cache-Control no-store, Content-Security-Policy default-src 'self'; frame-ancestors 'none'; form-action 'self', X-Content-Type-Options nosniff, Referrer-Policy no-referrer, and Permissions-Policy denying camera, microphone, geolocation, and payment.

prepare validates the target exists and returns a random confirmation id, exact human phrase, impact summary, and 120-second expiry. confirm requires matching session, CSRF, phrase, expiry, and idempotency key. Persist the pending record and result. Commands call the existing strict Hermes command service, not shell commands.

- [ ] **Step 4: Run API tests**

Run: uv run pytest tests/remote/test_api.py tests/remote/test_commands.py -v

Expected: all tests pass, including unauthenticated, expired, replay, wrong-target, forbidden-intent, and duplicate-submit cases.

- [ ] **Step 5: Commit**

    git add src/hermes_orchestrator/remote/api.py src/hermes_orchestrator/remote/commands.py tests/remote/test_api.py tests/remote/test_commands.py
    git commit -m "feat: confirm restricted remote commands"

### Task 4: Add the mobile operations console

**Files:**
- Create: src/hermes_orchestrator/remote/templates/login.html
- Create: src/hermes_orchestrator/remote/templates/dashboard.html
- Create: src/hermes_orchestrator/remote/templates/project.html
- Create: src/hermes_orchestrator/remote/templates/confirm.html
- Create: src/hermes_orchestrator/remote/static/app.css
- Modify: src/hermes_orchestrator/remote/api.py
- Test: tests/remote/test_pages.py

**Interfaces:**
- Consumes: StatusViewService and confirmation endpoints
- Produces: GET /login, POST /login, POST /logout, GET /, GET /projects/{project_key}, GET/POST /confirm/{confirmation_id}

- [ ] **Step 1: Write failing page tests**

    def test_dashboard_contains_operational_sections(browser_client):
        html = browser_client.get("/").text
        for label in ("Queue", "Projects", "Workers", "Resources", "Reviews", "Stalls"):
            assert label in html

    def test_page_has_mobile_viewport(browser_client):
        html = browser_client.get("/").text
        assert 'name="viewport"' in html
        assert "width=device-width" in html

    def test_forms_include_csrf_and_no_secret_inputs(browser_client):
        html = browser_client.get("/projects/demo").text
        assert 'name="csrf_token"' in html
        assert "environment" not in html.lower()
        assert "credential" not in html.lower()

- [ ] **Step 2: Run tests and verify they fail**

Run: uv run pytest tests/remote/test_pages.py -v

Expected: FAIL because the templates are absent.

- [ ] **Step 3: Implement a server-rendered phone-first console**

Use semantic HTML, no third-party scripts, no remote fonts, and no inline event handlers. Use a single-column layout below 720px, 44px minimum touch targets, system fonts, status text in addition to color, and sticky confirmation actions. Render only sanitized view models.

Dashboard actions are Status, Pause, Resume, Retry, Reprioritize, Checkpoint, Cleanup, Approve stall, and Approve handoff. Queue admission requires the exact Linear issue ID, project alias, priority, and a generated operator instruction id tied to the authenticated session.

- [ ] **Step 4: Run page tests and inspect on iPhone-sized viewport**

Run: uv run pytest tests/remote/test_pages.py -v

Then start the test server on 127.0.0.1:8787 and inspect 390x844 and 430x932 viewports. Expected: no horizontal scroll, all actions have readable confirmation text, and no secret or administrative route is linked.

- [ ] **Step 5: Commit**

    git add src/hermes_orchestrator/remote/templates src/hermes_orchestrator/remote/static src/hermes_orchestrator/remote/api.py tests/remote/test_pages.py
    git commit -m "feat: add the phone operations console"

### Task 5: Install loopback launchd services and Tailscale Serve

**Files:**
- Create: deploy/launchd/com.josystem.hermes-orchestrator.plist
- Create: deploy/launchd/com.josystem.hermes-operations.plist
- Create: scripts/install-launchd.sh
- Create: docs/operations/remote-access.md
- Test: tests/deploy/test_launchd.py
- Test: tests/deploy/test_remote_runbook.py

**Interfaces:**
- Produces: supervisor launchd service
- Produces: operations service running uvicorn on 127.0.0.1:8787
- Produces: tailscale serve --bg --yes http://127.0.0.1:8787

- [ ] **Step 1: Write failing deployment policy tests**

    def test_operations_plist_binds_loopback(plist):
        args = plist("com.josystem.hermes-operations")["ProgramArguments"]
        assert args[-4:] == ["--host", "127.0.0.1", "--port", "8787"]

    def test_plists_do_not_contain_secrets(plists):
        serialized = json.dumps(plists)
        for forbidden in ("TOKEN", "PASSWORD", "API_KEY", "PHONE"):
            assert forbidden not in serialized.upper()

    def test_runbook_forbids_funnel(runbook_text):
        assert "tailscale funnel" in runbook_text
        assert "must not" in runbook_text.lower()
        assert "tailscale serve reset" in runbook_text

- [ ] **Step 2: Run tests and verify they fail**

Run: uv run pytest tests/deploy/test_launchd.py tests/deploy/test_remote_runbook.py -v

Expected: FAIL because deployment artifacts do not exist.

- [ ] **Step 3: Implement services and the safe setup runbook**

Use /Users/josystem/hermes-orchestrator/.venv/bin/hermes-orchestrator as the program. Set WorkingDirectory to /Users/josystem/hermes-orchestrator, RunAtLoad=true, KeepAlive successful-exit=false, and separate bounded log files under ~/.local/state/hermes-orchestrator/logs.

install-launchd.sh validates absolute paths, creates only the user LaunchAgents and log directory, runs plutil -lint, bootstraps the two explicit labels, and prints status commands. It does not unload unrelated agents.

The runbook order is:

1. Run hermes-orchestrator remote-auth-init and save the displayed token in the iPhone password manager.
2. Install and start both launchd services.
3. Verify curl http://127.0.0.1:8787/healthz succeeds and an unauthenticated /api/status returns 401.
4. Run /usr/local/bin/tailscale serve --bg --yes http://127.0.0.1:8787.
5. Verify tailscale serve status --json names only 127.0.0.1:8787 and no full Hermes dashboard port.
6. Open the MagicDNS HTTPS URL on the iPhone and authenticate.
7. Disable remote access with /usr/local/bin/tailscale serve reset without stopping local orchestration.

- [ ] **Step 4: Run deployment tests and local health checks**

Run: uv run pytest tests/deploy -v && plutil -lint deploy/launchd/*.plist

Expected: all tests pass and both plists are valid.

- [ ] **Step 5: Commit**

    git add deploy scripts/install-launchd.sh docs/operations/remote-access.md tests/deploy
    git commit -m "ops: deploy tailnet-only operations console"

### Task 6: Add the restricted Photon command channel

**Files:**
- Create: src/hermes_orchestrator/remote/photon.py
- Create: config/photon-policy.yaml
- Modify: src/hermes_orchestrator/hermes_tools.py
- Test: tests/remote/test_photon.py
- Create: docs/operations/photon.md

**Interfaces:**
- Produces: PhotonPolicy.authorize(PhotonEnvelope) -> AuthorizationDecision
- Produces: PhotonCommandParser.parse(text: str) -> PhotonCommand
- Produces: PhotonGateway.handle(envelope: PhotonEnvelope) -> PhotonReply
- Produces commands: status, queue, pause, resume, approve, retry, priority, checkpoint, cleanup

- [ ] **Step 1: Write failing sender and grammar tests**

    def test_rejects_non_allowlisted_sender(gateway):
        reply = gateway.handle(envelope(sender="+15550000000", text="status"))
        assert reply.code == "sender_not_allowed"

    def test_rejects_group_conversation(gateway):
        reply = gateway.handle(
            envelope(sender=operator_number(), text="status", is_group=True)
        )
        assert reply.code == "groups_not_allowed"

    @pytest.mark.parametrize("text", [
        "shell ls", "show env", "credentials", "mcp list", "force delete demo"
    ])
    def test_rejects_administrative_text(gateway, text):
        assert gateway.handle(envelope(sender=operator_number(), text=text)).code == (
            "command_not_allowed"
        )

    def test_pause_requires_targeted_confirmation(gateway):
        first = gateway.handle(envelope(sender=operator_number(), text="pause demo"))
        assert first.text == "Reply PAUSE DEMO within 2 minutes to confirm."
        second = gateway.handle(envelope(sender=operator_number(), text="PAUSE DEMO"))
        assert second.code == "executed"

- [ ] **Step 2: Run tests and verify they fail**

Run: uv run pytest tests/remote/test_photon.py -v

Expected: FAIL because Photon policy and parser are undefined.

- [ ] **Step 3: Implement allowlisting, grammar, and low-sensitivity replies**

Store the normalized operator number only in Keychain service hermes-orchestrator-photon account operator-number. Store an HMAC of it in runtime state for matching and auditing. Require is_group=false and telemetry_enabled=false.

Accept only these exact forms:

    status
    queue ISSUE_ID PROJECT_ALIAS PRIORITY
    pause PROJECT_ALIAS
    resume PROJECT_ALIAS
    retry ISSUE_ID
    priority ISSUE_ID PRIORITY
    approve STALL_OR_HANDOFF_ID
    checkpoint PROJECT_ALIAS
    cleanup PROJECT_ALIAS

Mutations require an exact uppercase confirmation phrase within 120 seconds. Replies contain aliases, states, ages, resource bands, issue IDs, PR numbers, and short error codes only. Truncate at 800 characters and direct the operator to the phone console for detail.

- [ ] **Step 4: Run Photon policy tests**

Run: uv run pytest tests/remote/test_photon.py -v

Expected: all tests pass, including number normalization, replay, expired confirmation, group, unknown command, oversized message, and telemetry-enabled denial.

- [ ] **Step 5: Commit**

    git add src/hermes_orchestrator/remote/photon.py config/photon-policy.yaml src/hermes_orchestrator/hermes_tools.py tests/remote/test_photon.py docs/operations/photon.md
    git commit -m "feat: restrict Photon operations commands"

### Task 7: Configure Photon interactively and run the remote security acceptance test

**Files:**
- Test: tests/integration/test_remote_security_acceptance.py
- Modify: docs/operations/photon.md
- Modify: README.md

**Interfaces:**
- Consumes: Hermes Photon setup, operations API, PhotonGateway
- Produces: verified iPhone and Apple Watch notification and command path

- [ ] **Step 1: Write the failing end-to-end security test**

    def test_remote_surfaces_never_reach_admin_capabilities(remote_acceptance):
        remote_acceptance.login()
        assert remote_acceptance.phone.status().status_code == 200
        for intent in (
            "shell", "credentials", "provider_auth", "mcp_admin",
            "read_env", "force_delete", "spend"
        ):
            assert remote_acceptance.phone.prepare(intent).status_code in (400, 403)
            assert remote_acceptance.photon.send(intent).code == "command_not_allowed"
        assert remote_acceptance.full_hermes_dashboard_via_tailnet().status_code != 200

    def test_remote_pause_is_confirmed_and_audited_once(remote_acceptance):
        pending = remote_acceptance.phone.prepare("pause", "project:demo")
        result = remote_acceptance.phone.confirm(pending, key="remote-pause-1")
        duplicate = remote_acceptance.phone.confirm(pending, key="remote-pause-1")
        assert result == duplicate
        assert remote_acceptance.audit.count("remote.command.executed") == 1

- [ ] **Step 2: Run tests and verify the live-path fixture is disabled**

Run: uv run pytest tests/integration/test_remote_security_acceptance.py -v

Expected: simulated tests pass and the live Photon test is skipped until PHOTON_LIVE_TEST=1 is set interactively.

- [ ] **Step 3: Perform interactive Photon setup without recording secrets**

Run hermes photon install-sidecar, confirm hermes photon telemetry reports off, then run hermes photon setup --phone OPERATOR_PHONE in an interactive terminal. Complete device login and project selection in the browser. Do not place the number, project secret, token, or assigned number in shell history, Git, test output, or the orchestration database.

Enter the operator number into Keychain through hermes-orchestrator photon-allowlist-init, then verify hermes photon status reports configured values and telemetry off without copying its sensitive values into the repository.

- [ ] **Step 4: Run Phase 5 verification**

Run: uv run pytest -q && uv run ruff check .

Then execute the documented live checks from the iPhone and Apple Watch:

1. Receive a resource-warning notification.
2. Request status.
3. Attempt a group command and observe rejection.
4. Pause a test project with two-message confirmation.
5. Verify the audit contains one execution and no phone number or message body.
6. Confirm the full Hermes dashboard is not reachable through the tailnet URL.

Expected: automated tests pass and all six live checks succeed.

- [ ] **Step 5: Commit**

    git add tests/integration/test_remote_security_acceptance.py docs/operations/photon.md README.md
    git commit -m "test: verify restricted remote operations"

## Phase exit gate

The system is ready for routine remote operation only when:

- Tailscale Serve exposes only the loopback operations service and Funnel is disabled.
- Application auth, Secure cookies, CSRF, rate limits, confirmations, and idempotency all pass.
- The console works at iPhone viewport sizes and exposes no administrative routes.
- The full Hermes dashboard remains local-only.
- Photon telemetry is off, only the operator number is allowlisted, and groups are rejected.
- Apple Watch status and confirmation replies work without including sensitive content.
- Disabling Tailscale Serve and Photon commands leaves local orchestration running normally.
