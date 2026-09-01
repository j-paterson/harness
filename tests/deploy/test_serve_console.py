"""In-process integration tests for the serve-console production wiring.

This module covers the INFRA-177 round-3 console slice: a uvicorn server
thread on an ephemeral loopback port serving the app built by the real
``build_console_dependencies``, over durable seeded state — an admitted
issue, a retryable blocked issue, an active project cell with current
checkpoint-safety evidence, a pending stall consultation, a review with its
CI ledger row, and a submitted handoff. Every action the production console
renders (queue, reprioritize, retry, checkpoint, approve-stall) is executed
through prepare and confirm with a durable-effect assertion, and every
removed capability (pause, resume, cleanup, approve-handoff) is proven to
neither render nor be preparable.

The ONLY substituted boundary is the macOS-keychain I/O object: an isolated
in-memory ``SecretStore`` seeded exclusively through the real
``RemoteCredentialService.initialize()``. Everything above that boundary is
production code. Nothing here touches the login keychain, the live state
database, or ports 8787/8788.

Tests sharing the module-scoped harness run in file order: read-surface
assertions come before the mutations that consume the seeded rows.
"""

from __future__ import annotations

import contextlib
import re
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

import httpx
import pytest
import uvicorn

from hermes_orchestrator.checkpoints import CheckpointSafetyStore
from hermes_orchestrator.db import Database
from hermes_orchestrator.deploy.launchd import ServiceSpec, standard_inventory
from hermes_orchestrator.domain import AdmissionRequest, IssueState
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.handoffs import HandoffDocument, HandoffService, HandoffTest
from hermes_orchestrator.queue import QueueService
from hermes_orchestrator.remote.api import RemoteDependencies, create_operations_app
from hermes_orchestrator.remote.auth import (
    SESSION_COOKIE_NAME,
    RemoteCredentialService,
)
from hermes_orchestrator.remote.serve import (
    _ConsoleTargetCatalog,
    build_console_dependencies,
)
from hermes_orchestrator.stalls import (
    PlaybookService,
    Remedy,
    StallDiagnosis,
    predicate_key_for,
)
from tests.remote.test_auth import FakeKeychain

GITHUB_REPOS = {"demo": "j-paterson/hermes-orchestrator"}
ISSUE_ID = "demo-231"
BLOCKED_ISSUE_ID = "demo-232"
CONSOLE_QUEUED_ISSUE_ID = "demo-233"
STUCK_ISSUE_ID = "demo-234"
SEEDED_PRIORITY = 3
CELL_ID = "cell-demo-1"
CELL_SESSION_ID = "lead-session-demo-1"
PROFILE_ALIAS = "profile-demo-a"
PR_NUMBER = 41
MERGE_SHA = "1234abcd1234abcd"
PR_URL = f"https://github.com/j-paterson/hermes-orchestrator/pull/{PR_NUMBER}"
# The production action surface: exactly these five render and prepare.
AVAILABLE_SLUGS = ("queue", "reprioritize", "retry", "checkpoint", "approve-stall")
# Removed capabilities: no durable handler can be wired safely, so they
# must neither render nor be preparable.
REMOVED = (
    ("pause", "pause"),
    ("resume", "resume"),
    ("cleanup", "request_cleanup"),
    ("approve-handoff", "approve_handoff"),
)
# The session fingerprint is "<client-host>|<user-agent>", so every request
# in a flow presents this exact User-Agent.
USER_AGENT = "hermes-serve-console-tests/1.0"


@dataclass(frozen=True)
class ConsoleHarness:
    """Everything a test needs to drive the served console."""

    url: str
    credential: str
    dependencies: RemoteDependencies
    database_path: Path
    playbook_path: Path
    consultation_id: str
    handoff_id: str


@pytest.fixture(scope="module")
def console(tmp_path_factory: pytest.TempPathFactory):
    store = FakeKeychain()
    # The sole seeding path: the REAL credential initialization against the
    # isolated in-memory store.
    credential = RemoteCredentialService(keychain=store).initialize()
    state_dir = tmp_path_factory.mktemp("serve-console")
    database_path = state_dir / "state.db"
    playbook_path = state_dir / "playbooks.yaml"
    holder: dict[str, object] = {}
    configured = threading.Event()

    def serve() -> None:
        # The sqlite connection is single-thread; the database is opened,
        # seeded, used, and closed entirely inside the serving thread.
        database = Database.open(database_path)
        try:
            events = EventStore(database)
            queue = QueueService(database, events, GITHUB_REPOS.keys())
            queue.admit(
                AdmissionRequest(
                    issue_id=ISSUE_ID,
                    project_key="demo",
                    linear_priority=SEEDED_PRIORITY,
                    admitted_by="operator",
                    instruction_id="operator-instruction-1",
                )
            )
            # A second issue parked in a retryable state through the real
            # queue transitions.
            queue.admit(
                AdmissionRequest(
                    issue_id=BLOCKED_ISSUE_ID,
                    project_key="demo",
                    linear_priority=2,
                    admitted_by="operator",
                    instruction_id="operator-instruction-2",
                )
            )
            queue.transition(
                BLOCKED_ISSUE_ID,
                IssueState.BLOCKED,
                actor="operator",
                reason="dependency wait",
            )
            # A third issue reproducing the INFRA-198 stuck-queued bug:
            # admitted ready, paused by the orchestrator, then its
            # dependency_ready cleared out-of-band (no journaled event —
            # the observed production condition) before ever being
            # requeued.
            queue.admit(
                AdmissionRequest(
                    issue_id=STUCK_ISSUE_ID,
                    project_key="demo",
                    linear_priority=2,
                    admitted_by="operator",
                    instruction_id="operator-instruction-3",
                )
            )
            queue.transition(
                STUCK_ISSUE_ID,
                IssueState.PAUSED,
                actor="orchestrator",
                reason="resource pressure",
            )
            with database.transaction() as connection:
                connection.execute(
                    "UPDATE admitted_issues SET dependency_ready = 0 "
                    "WHERE issue_id = ?",
                    (STUCK_ISSUE_ID,),
                )
            now = datetime.now(UTC).isoformat()
            # project_cells is daemon-owned (the console only reads it), so
            # the active lead cell is seeded with an explicit INSERT using
            # the exact 0001-migration columns.
            with database.transaction() as connection:
                connection.execute(
                    "INSERT INTO project_cells("
                    "cell_id, project_key, state, profile_alias, session_id, "
                    "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        CELL_ID,
                        "demo",
                        "active",
                        PROFILE_ALIAS,
                        CELL_SESSION_ID,
                        now,
                        now,
                    ),
                )
            safety = CheckpointSafetyStore(database, events)
            safety.mark_safe(
                CELL_ID,
                CELL_SESSION_ID,
                boundary_kind="clean_worktree",
                evidence_id="boundary-proof-1",
            )
            playbooks = PlaybookService(
                database, events, playbook_path=playbook_path
            )
            predicate = {"signal": "ci_failure"}
            consultation = playbooks.consult(
                StallDiagnosis(
                    reason="ci_failure",
                    predicate=predicate,
                    predicate_key=predicate_key_for("ci_failure", predicate),
                    project_key="demo",
                    summary="ci failure reported",
                ),
                proposed=Remedy(
                    actions=("rerun_tests",),
                    verification="rerun_tests",
                    timeout_seconds=600,
                    rollback="none",
                ),
            )
            holder["consultation_id"] = consultation.consultation_id
            # reviews and ci_merge_ledger are daemon-owned tables (the merge
            # flow writes them); the review-with-passed-CI pair is seeded
            # with explicit INSERTs using the 0008/0007 migration columns.
            with database.transaction() as connection:
                connection.execute(
                    "INSERT INTO reviews("
                    "review_id, project_key, issue_id, event_id, repository, "
                    "branch, pr_number, reviewed_sha, state, merge_sha, "
                    "reason, projection_json, created_at, updated_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)",
                    (
                        "review-demo-1",
                        "demo",
                        ISSUE_ID,
                        "event-demo-1",
                        GITHUB_REPOS["demo"],
                        "feature/demo-231",
                        PR_NUMBER,
                        "candidate-sha-1",
                        "approved",
                        MERGE_SHA,
                        now,
                        now,
                    ),
                )
                connection.execute(
                    "INSERT INTO ci_merge_ledger("
                    "project_key, merge_sha, repository, pr_number, "
                    "integration_branch, candidate_sha, candidate_branch, "
                    "state, reason, packet_json, recorded_at, updated_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)",
                    (
                        "demo",
                        MERGE_SHA,
                        GITHUB_REPOS["demo"],
                        PR_NUMBER,
                        "main",
                        "candidate-sha-1",
                        "feature/demo-231",
                        "passed",
                        now,
                        now,
                    ),
                )
            # The real handoff flow: a request records intent, submit stores
            # the durable handoff row the target catalog reads.
            handoff_service = HandoffService(database)
            handoff_service.request(CELL_ID, "safe boundary rotation")
            record = handoff_service.submit(
                HandoffDocument(
                    cell_id=CELL_ID,
                    objective="finish demo-231",
                    status="review ready",
                    decisions=["kept the change minimal"],
                    branch="feature/demo-231",
                    commits=["abc1234 initial work"],
                    pull_request=PR_URL,
                    modified_files=["src/demo.py"],
                    tests=[
                        HandoffTest(command="uv run pytest", outcome="passed")
                    ],
                    blockers=[],
                    remaining_steps=["merge after review"],
                    commands=["uv run pytest"],
                    environment_notes=["standard macos host"],
                    risks=[],
                    next_action="merge the approved pull request",
                )
            )
            holder["handoff_id"] = record.handoff_id
            dependencies = build_console_dependencies(
                database=database,
                github_repos=GITHUB_REPOS,
                secret_store=store,
                playbook_path=playbook_path,
            )
            holder["dependencies"] = dependencies
            config = uvicorn.Config(
                create_operations_app(dependencies),
                host="127.0.0.1",
                port=0,
                log_level="warning",
            )
            server = uvicorn.Server(config)
            holder["server"] = server
            configured.set()
            server.run()
        finally:
            database.close()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    assert configured.wait(timeout=15), "server thread never configured"
    server = holder["server"]
    assert isinstance(server, uvicorn.Server)
    deadline = time.monotonic() + 15
    while not server.started:
        if time.monotonic() > deadline:
            raise RuntimeError("uvicorn did not start inside the deadline")
        time.sleep(0.05)
    port = server.servers[0].sockets[0].getsockname()[1]
    dependencies = holder["dependencies"]
    assert isinstance(dependencies, RemoteDependencies)
    yield ConsoleHarness(
        url=f"http://127.0.0.1:{port}",
        credential=credential,
        dependencies=dependencies,
        database_path=database_path,
        playbook_path=playbook_path,
        consultation_id=str(holder["consultation_id"]),
        handoff_id=str(holder["handoff_id"]),
    )
    server.should_exit = True
    thread.join(timeout=15)


def _client(harness: ConsoleHarness) -> httpx.Client:
    return httpx.Client(
        base_url=harness.url,
        headers={"user-agent": USER_AGENT},
        follow_redirects=False,
    )


def _login(client: httpx.Client, credential: str) -> str:
    """Authenticate through the real login flow; return the cookie value."""

    response = client.post("/login", data={"passcode": credential})
    assert response.status_code == 303
    set_cookie = response.headers["set-cookie"]
    assert set_cookie.startswith(f"{SESSION_COOKIE_NAME}=")
    value = set_cookie.split(";", 1)[0].split("=", 1)[1]
    # The Secure attribute keeps httpx's jar from replaying the header
    # cookie over plain loopback HTTP; carry the value explicitly.
    client.cookies.set(SESSION_COOKIE_NAME, value)
    return value


def _csrf(harness: ConsoleHarness, cookie_value: str) -> str:
    """A token from the SAME production CsrfService the server verifies with."""

    claims = harness.dependencies.sessions.verify(
        cookie_value, f"127.0.0.1|{USER_AGENT}"
    )
    return harness.dependencies.csrf.issue(claims.session_id)


def _prepare(
    client: httpx.Client,
    token: str,
    *,
    intent: str,
    target: str,
    parameters: dict[str, object] | None = None,
) -> httpx.Response:
    return client.post(
        "/api/commands/prepare",
        json={
            "intent": intent,
            "target": target,
            "parameters": parameters or {},
        },
        headers={"x-csrf-token": token},
    )


def _confirm(
    client: httpx.Client,
    token: str,
    pending: dict[str, object],
    idempotency_key: str,
) -> httpx.Response:
    return client.post(
        "/api/commands/confirm",
        json={
            "confirmation_id": pending["confirmation_id"],
            "confirmation_phrase": pending["confirmation_phrase"],
            "idempotency_key": idempotency_key,
        },
        headers={"x-csrf-token": token},
    )


def _query_one(
    database_path: Path, sql: str, parameters: tuple[object, ...] = ()
) -> tuple | None:
    # An independent read-only connection sees only committed state, so a
    # matched row proves the mutation survived its transaction.
    connection = sqlite3.connect(database_path)
    try:
        return connection.execute(sql, parameters).fetchone()
    finally:
        connection.close()


def _hidden(html: str, name: str) -> str:
    match = re.search(rf'name="{name}" value="([^"]+)"', html)
    assert match is not None, f"hidden field {name} not rendered"
    return match.group(1)


# ---------------------------------------------------------------------------
# Read surface
# ---------------------------------------------------------------------------


def test_healthz_is_unauthenticated_ok_with_security_headers(
    console: ConsoleHarness,
) -> None:
    with _client(console) as client:
        response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"code": "ok"}
    # The middleware applies to every response, /healthz included.
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["referrer-policy"] == "no-referrer"


def test_status_without_a_session_is_unauthorized(console: ConsoleHarness) -> None:
    with _client(console) as client:
        response = client.get("/api/status")
    assert response.status_code == 401
    assert response.json() == {"code": "unauthorized"}


def test_login_then_sanitized_status_shows_seeded_durable_state(
    console: ConsoleHarness,
) -> None:
    with _client(console) as client:
        _login(client, console.credential)
        response = client.get("/api/status")
    assert response.status_code == 200
    body = response.json()
    assert [item["project_alias"] for item in body["projects"]] == ["demo"]
    assert body["projects"][0]["state"] == "registered"
    queue_states = {
        item["issue_alias"]: item["state"] for item in body["queue"]
    }
    assert queue_states[ISSUE_ID] == "queued"
    assert queue_states[BLOCKED_ISSUE_ID] == "blocked"
    # The seeded active lead cell renders as a sanitized worker row.
    (worker,) = body["workers"]
    assert worker["worker_alias"] == CELL_ID
    assert worker["profile_alias"] == PROFILE_ALIAS
    assert worker["project_alias"] == "demo"
    assert worker["state"] == "active"
    assert worker["age_seconds"] >= 0
    assert worker["context_band"] == "unknown"
    # The seeded review renders with the SYNTHESIZED configured pr_url and
    # the CI ledger outcome joined by (project_key, merge_sha).
    (review,) = body["reviews"]
    assert review["issue_alias"] == ISSUE_ID
    assert review["project_alias"] == "demo"
    assert review["state"] == "approved"
    assert review["pr_url"] == PR_URL
    assert review["ci_status"] == "passed"
    # The pending consultation renders as a stall row; the single active
    # cell with current safe evidence makes it checkpoint-eligible.
    (stall,) = body["stalls"]
    assert stall["worker_alias"] == console.consultation_id
    assert stall["project_alias"] == "demo"
    assert stall["stall_summary"] == "ci_failure"
    assert stall["checkpoint_eligible"] is True
    resources = body["resources"]
    assert resources is not None
    assert resources["band"] in {"unknown", "green", "yellow", "red"}
    assert resources["total_memory_gib"] > 0


def test_console_page_renders_rows_and_only_available_actions(
    console: ConsoleHarness,
) -> None:
    with _client(console) as client:
        _login(client, console.credential)
        response = client.get("/console")
    assert response.status_code == 200
    page = response.text
    for slug in AVAILABLE_SLUGS:
        assert f'"/console/actions/{slug}"' in page
    for slug, _intent in REMOVED:
        assert f'"/console/actions/{slug}"' not in page
    assert CELL_ID in page
    assert PROFILE_ALIAS in page
    assert PR_URL in page
    assert console.consultation_id in page


def test_project_page_renders_the_seeded_rows(console: ConsoleHarness) -> None:
    with _client(console) as client:
        _login(client, console.credential)
        response = client.get("/projects/demo")
    assert response.status_code == 200
    page = response.text
    assert CELL_ID in page
    assert ISSUE_ID in page
    assert PR_URL in page
    assert console.consultation_id in page
    assert "checkpoint eligible" in page


def test_target_catalog_resolves_the_durable_handoff(
    console: ConsoleHarness,
) -> None:
    # The catalog is exercised over its own connection because prepare can
    # never reach a handoff target while approve_handoff stays unwired.
    database = Database.open(console.database_path)
    try:
        catalog = _ConsoleTargetCatalog(
            database=database, project_aliases=frozenset(GITHUB_REPOS)
        )
        assert catalog.exists("handoff", console.handoff_id) is True
        assert catalog.exists("handoff", "absent-handoff") is False
        assert catalog.exists("project", "demo") is True
        assert catalog.exists("issue", ISSUE_ID) is True
        assert catalog.exists("worker", CELL_ID) is False
    finally:
        database.close()


# ---------------------------------------------------------------------------
# Every rendered action, executed through prepare + confirm
# ---------------------------------------------------------------------------


def test_confirmed_reprioritize_executes_and_persists(
    console: ConsoleHarness,
) -> None:
    new_priority = 1
    with _client(console) as client:
        cookie_value = _login(client, console.credential)
        token = _csrf(console, cookie_value)
        prepared = _prepare(
            client,
            token,
            intent="reprioritize",
            target=f"issue:{ISSUE_ID}",
            parameters={"priority": new_priority},
        )
        assert prepared.status_code == 200
        pending = prepared.json()
        assert pending["confirmation_phrase"] == (
            f"REPRIORITIZE {ISSUE_ID.upper()}"
        )
        confirmed = _confirm(
            client, token, pending, "serve-console-reprioritize-1"
        )
    assert confirmed.status_code == 200
    result = confirmed.json()
    assert result["code"] == "reprioritized"
    assert result["state"] == {"issue_id": ISSUE_ID, "priority": new_priority}
    row = _query_one(
        console.database_path,
        "SELECT priority FROM admitted_issues WHERE issue_id = ?",
        (ISSUE_ID,),
    )
    assert row is not None
    assert row[0] == new_priority


def test_confirmed_retry_requeues_the_blocked_issue(
    console: ConsoleHarness,
) -> None:
    with _client(console) as client:
        cookie_value = _login(client, console.credential)
        token = _csrf(console, cookie_value)
        prepared = _prepare(
            client, token, intent="retry", target=f"issue:{BLOCKED_ISSUE_ID}"
        )
        assert prepared.status_code == 200
        pending = prepared.json()
        assert pending["confirmation_phrase"] == (
            f"RETRY {BLOCKED_ISSUE_ID.upper()}"
        )
        confirmed = _confirm(client, token, pending, "serve-console-retry-1")
    assert confirmed.status_code == 200
    result = confirmed.json()
    assert result["code"] == "accepted"
    assert result["state"] == {"issue_id": BLOCKED_ISSUE_ID, "state": "queued"}
    row = _query_one(
        console.database_path,
        "SELECT state FROM admitted_issues WHERE issue_id = ?",
        (BLOCKED_ISSUE_ID,),
    )
    assert row is not None
    assert row[0] == "queued"


def test_retry_of_a_queued_issue_is_rejected_with_a_static_reason(
    console: ConsoleHarness,
) -> None:
    # queue.transition itself is any-to-any; the retry bound lives in the
    # console handler, so a non-retryable state must surface the static
    # rejection code through confirm.
    with _client(console) as client:
        cookie_value = _login(client, console.credential)
        token = _csrf(console, cookie_value)
        prepared = _prepare(
            client, token, intent="retry", target=f"issue:{ISSUE_ID}"
        )
        assert prepared.status_code == 200
        confirmed = _confirm(
            client, token, prepared.json(), "serve-console-retry-2"
        )
    assert confirmed.status_code == 200
    result = confirmed.json()
    assert result["code"] == "rejected"
    assert result["state"] == {"reason": "retry_not_applicable"}
    row = _query_one(
        console.database_path,
        "SELECT state FROM admitted_issues WHERE issue_id = ?",
        (ISSUE_ID,),
    )
    assert row is not None
    assert row[0] == "queued"


def test_confirmed_retry_restores_dependency_readiness_after_a_cleared_pause_hold(
    console: ConsoleHarness,
) -> None:
    # INFRA-198: STUCK_ISSUE_ID was seeded paused with dependency_ready
    # cleared out-of-band. The same production retry command must both
    # requeue it AND repair readiness — there is no other supported path.
    with _client(console) as client:
        cookie_value = _login(client, console.credential)
        token = _csrf(console, cookie_value)
        prepared = _prepare(
            client, token, intent="retry", target=f"issue:{STUCK_ISSUE_ID}"
        )
        assert prepared.status_code == 200
        pending = prepared.json()
        assert pending["confirmation_phrase"] == f"RETRY {STUCK_ISSUE_ID.upper()}"
        confirmed = _confirm(client, token, pending, "serve-console-retry-3")
    assert confirmed.status_code == 200
    result = confirmed.json()
    assert result["code"] == "accepted"
    assert result["state"] == {"issue_id": STUCK_ISSUE_ID, "state": "queued"}
    row = _query_one(
        console.database_path,
        "SELECT state, dependency_ready FROM admitted_issues WHERE issue_id = ?",
        (STUCK_ISSUE_ID,),
    )
    assert row is not None
    assert row[0] == "queued"
    assert row[1] == 1
    event_row = _query_one(
        console.database_path,
        "SELECT count(*) FROM events WHERE event_type = 'issue.dependency_ready' "
        "AND aggregate_id = ?",
        (STUCK_ISSUE_ID,),
    )
    assert event_row is not None
    assert event_row[0] == 1

    # Semantics preserved: now that the row is healthy and queued, a
    # second retry is still rejected with the same static reason code.
    with _client(console) as client:
        cookie_value = _login(client, console.credential)
        token = _csrf(console, cookie_value)
        prepared = _prepare(
            client, token, intent="retry", target=f"issue:{STUCK_ISSUE_ID}"
        )
        assert prepared.status_code == 200
        confirmed = _confirm(
            client, token, prepared.json(), "serve-console-retry-4"
        )
    assert confirmed.status_code == 200
    result = confirmed.json()
    assert result["code"] == "rejected"
    assert result["state"] == {"reason": "retry_not_applicable"}


def test_confirmed_checkpoint_persists_a_pending_request(
    console: ConsoleHarness,
) -> None:
    with _client(console) as client:
        cookie_value = _login(client, console.credential)
        token = _csrf(console, cookie_value)
        prepared = _prepare(
            client, token, intent="request_checkpoint", target="project:demo"
        )
        assert prepared.status_code == 200
        confirmed = _confirm(
            client, token, prepared.json(), "serve-console-checkpoint-1"
        )
    assert confirmed.status_code == 200
    result = confirmed.json()
    assert result["code"] == "accepted"
    assert result["state"]["state"] == "pending"
    row = _query_one(
        console.database_path,
        "SELECT cell_id, session_id, state FROM checkpoint_requests "
        "WHERE state = 'pending'",
    )
    assert row is not None
    assert row[0] == CELL_ID
    assert row[1] == CELL_SESSION_ID


def test_confirmed_approve_stall_approves_and_writes_the_playbook(
    console: ConsoleHarness,
) -> None:
    with _client(console) as client:
        cookie_value = _login(client, console.credential)
        token = _csrf(console, cookie_value)
        prepared = _prepare(
            client, token, intent="approve_stall", target="project:demo"
        )
        assert prepared.status_code == 200
        confirmed = _confirm(
            client, token, prepared.json(), "serve-console-approve-stall-1"
        )
    assert confirmed.status_code == 200
    result = confirmed.json()
    assert result["code"] == "accepted"
    assert result["state"] == {
        "consultation_id": console.consultation_id,
        "approved": True,
    }
    row = _query_one(
        console.database_path,
        "SELECT state FROM stall_consultations WHERE consultation_id = ?",
        (console.consultation_id,),
    )
    assert row is not None
    assert row[0] == "approved"
    # The approval wrote the versioned playbook file atomically.
    assert console.playbook_path.exists()
    assert "rerun_tests" in console.playbook_path.read_text()


def test_console_queue_form_flow_admits_a_new_issue(
    console: ConsoleHarness,
) -> None:
    # The full rendered HTML flow: form -> prepare -> typed-phrase confirm,
    # exercising the operator_instruction_id the console mints per prepare.
    with _client(console) as client:
        _login(client, console.credential)
        form_page = client.get("/console/actions/queue")
        assert form_page.status_code == 200
        prepared = client.post(
            "/console/actions/queue/prepare",
            data={
                "csrf": _hidden(form_page.text, "csrf"),
                "target": "demo",
                "issue_id": CONSOLE_QUEUED_ISSUE_ID,
                "priority": "2",
            },
        )
        assert prepared.status_code == 200
        phrase = re.search(r"<strong>([^<]+)</strong>", prepared.text)
        assert phrase is not None
        confirmed = client.post(
            "/console/actions/queue/confirm",
            data={
                "csrf": _hidden(prepared.text, "csrf"),
                "confirmation_id": _hidden(prepared.text, "confirmation_id"),
                "idempotency_key": _hidden(prepared.text, "idempotency_key"),
                "confirmation_phrase": phrase.group(1),
            },
        )
    assert confirmed.status_code == 200
    assert "queued" in confirmed.text
    row = _query_one(
        console.database_path,
        "SELECT priority, state, instruction_id FROM admitted_issues "
        "WHERE issue_id = ?",
        (CONSOLE_QUEUED_ISSUE_ID,),
    )
    assert row is not None
    assert row[0] == 2
    assert row[1] == "queued"
    # The instruction identity was minted by the console, never typed.
    assert str(row[2]).startswith("console-")


# ---------------------------------------------------------------------------
# Removed capabilities fail closed everywhere
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("slug", "intent"), REMOVED)
def test_removed_capability_form_is_unknown_and_prepare_is_unsupported(
    console: ConsoleHarness, slug: str, intent: str
) -> None:
    with _client(console) as client:
        cookie_value = _login(client, console.credential)
        form = client.get(f"/console/actions/{slug}")
        assert form.status_code == 404
        assert "unknown_action" in form.text
        posted = client.post(f"/console/actions/{slug}/prepare", data={})
        assert posted.status_code == 404
        assert "unknown_action" in posted.text
        token = _csrf(console, cookie_value)
        target = (
            f"handoff:{console.handoff_id}"
            if intent == "approve_handoff"
            else "project:demo"
        )
        prepared = _prepare(client, token, intent=intent, target=target)
    assert prepared.status_code == 400
    assert prepared.json() == {"code": "unsupported_intent"}


# ---------------------------------------------------------------------------
# Generated-job subprocess fixture (lead slice): the exact ProgramArguments
# of every inventory service launched directly as child processes — never
# via launchctl — against an isolated tmp state dir, a tmp config repo, and
# an ephemeral loopback port. This proves the generated deployment passes
# its own preflight shape: the commands start, stay running, and the
# operations job serves its health and auth boundary. The child process
# reads the REAL keychain for its remote credential, so authenticated
# subprocess flows are out of scope here; the identical
# build_console_dependencies graph is driven end to end by the in-process
# suite above.
# ---------------------------------------------------------------------------

PROJECTS_YAML = """\
projects:
  demo:
    linear_team: DEMO
    repo_path: /tmp/hermes-serve-console-tests/demo
    integration_branch: main
    github_repo: j-paterson/hermes-orchestrator
    ci: none
"""


def _free_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@pytest.fixture()
def generated_inventory(
    tmp_path: Path,
) -> tuple[tuple[ServiceSpec, ...], int]:
    binary = Path(sys.executable).parent / "hermes-orchestrator"
    assert binary.exists(), "console script must be installed in the venv"
    config_repo = tmp_path / "config-repo"
    (config_repo / "config").mkdir(parents=True)
    (config_repo / "config" / "projects.yaml").write_text(PROJECTS_YAML)
    # load_settings reads policies.yaml unconditionally; empty means the
    # default observe mode, so the children never load live credentials.
    (config_repo / "config" / "policies.yaml").write_text("")
    # serve-console resolves the playbook file under the config repo; an
    # empty document is the valid zero-playbook state.
    (config_repo / "config" / "playbooks.yaml").write_text("")
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    # Pre-migrate the isolated database so concurrently starting children
    # never race the schema migration (the operator flow also installs into
    # a state dir whose database already exists).
    Database.open(state_dir / "state.db").close()
    port = _free_loopback_port()
    inventory = standard_inventory(
        binary=PurePosixPath(binary),
        config_repo=PurePosixPath(config_repo),
        state_dir=PurePosixPath(state_dir),
        log_dir=PurePosixPath(log_dir),
        console_port=port,
    )
    return inventory, port


def test_generated_jobs_stay_running_and_serve_the_boundary(
    generated_inventory: tuple[tuple[ServiceSpec, ...], int], tmp_path: Path
) -> None:
    inventory, port = generated_inventory
    processes: list[subprocess.Popen[bytes]] = []
    log_paths: list[Path] = []
    try:
        for spec in inventory:
            out_path = tmp_path / f"{spec.label}.child.log"
            log_paths.append(out_path)
            with out_path.open("wb") as sink:
                processes.append(
                    subprocess.Popen(
                        list(spec.program_arguments),
                        stdout=sink,
                        stderr=subprocess.STDOUT,
                    )
                )
        deadline = time.monotonic() + 30
        healthy = False
        while time.monotonic() < deadline:
            try:
                response = httpx.get(
                    f"http://127.0.0.1:{port}/healthz", timeout=1.0
                )
            except httpx.HTTPError:
                time.sleep(0.2)
                continue
            if response.status_code == 200 and response.json() == {"code": "ok"}:
                healthy = True
                break
            time.sleep(0.2)
        diagnostics = {
            path.name: path.read_text(errors="replace")[-2000:]
            for path in log_paths
            if path.exists()
        }
        assert healthy, f"operations job never served /healthz: {diagnostics}"
        unauthenticated = httpx.get(
            f"http://127.0.0.1:{port}/api/status", timeout=5.0
        )
        assert unauthenticated.status_code == 401
        # The command surface is wired and fail-closed in the child too: an
        # unauthenticated prepare for a non-inline intent is refused at the
        # session boundary, not by a missing route.
        unauthenticated_prepare = httpx.post(
            f"http://127.0.0.1:{port}/api/commands/prepare",
            json={"intent": "retry", "target": "issue:none", "parameters": {}},
            timeout=5.0,
        )
        assert unauthenticated_prepare.status_code == 401
        assert unauthenticated_prepare.json() == {"code": "unauthorized"}
        # Both generated jobs are still running well after startup — they
        # did not exit on argparse errors or crash loops.
        time.sleep(1.0)
        for spec, process in zip(inventory, processes, strict=True):
            assert process.poll() is None, (
                spec.label,
                process.returncode,
                diagnostics,
            )
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            with contextlib.suppress(subprocess.TimeoutExpired):
                process.wait(timeout=10)


# ---------------------------------------------------------------------------
# Wrapper subprocess: the REAL cli serve-console code path in a child
# process — argument parsing, load_settings, Database.open,
# build_console_dependencies (with the config repo's playbooks.yaml), and
# run_console — with ONLY the sanctioned macOS-keychain I/O boundary
# substituted. The wrapper rebinds the Keychain symbol build_console_
# dependencies resolves to an inline dict-backed store, seeds it through
# the real RemoteCredentialService.initialize(), prints the credential
# once, then hands control to cli.main. Nothing above that boundary is
# faked, and no test module is imported into the child.
# ---------------------------------------------------------------------------

CHILD_WRAPPER = """\
import sys

from hermes_orchestrator import cli
from hermes_orchestrator.keychain import KeychainItemExists, KeychainItemMissing
from hermes_orchestrator.remote import serve
from hermes_orchestrator.remote.auth import RemoteCredentialService


class MemoryStore:
    # The sanctioned substitution: the keychain I/O object only, honouring
    # the real read/create fail-closed semantics.

    def __init__(self):
        self._items = {}

    def read(self, service, account):
        secret = self._items.get((service, account), "")
        if not secret:
            raise ValueError("keychain secret is empty")
        return secret

    def read_classified(self, service, account):
        if (service, account) not in self._items:
            raise KeychainItemMissing("keychain item not found")
        return self._items[(service, account)]

    def create(self, service, account, secret):
        if (service, account) in self._items:
            raise KeychainItemExists("keychain item already exists")
        self._items[(service, account)] = secret


store = MemoryStore()
serve.Keychain = lambda: store
print(RemoteCredentialService(keychain=store).initialize(), flush=True)
repo_root, state_dir, port = sys.argv[1:4]
sys.exit(
    cli.main(
        [
            "--repo-root",
            repo_root,
            "--state-dir",
            state_dir,
            "serve-console",
            "--host",
            "127.0.0.1",
            "--port",
            port,
        ]
    )
)
"""

RETRYABLE_CHILD_ISSUE_ID = "demo-501"


def _first_line(stream, timeout: float) -> str:
    """One blocking readline bounded by a timeout via a daemon thread."""

    lines: list[str] = []

    def read() -> None:
        lines.append(stream.readline())

    thread = threading.Thread(target=read, daemon=True)
    thread.start()
    thread.join(timeout)
    return lines[0].strip() if lines else ""


def _wait_healthy(port: int, deadline_seconds: float) -> bool:
    deadline = time.monotonic() + deadline_seconds
    while time.monotonic() < deadline:
        try:
            response = httpx.get(
                f"http://127.0.0.1:{port}/healthz", timeout=1.0
            )
        except httpx.HTTPError:
            time.sleep(0.2)
            continue
        if response.status_code == 200 and response.json() == {"code": "ok"}:
            return True
        time.sleep(0.2)
    return False


def test_wrapper_subprocess_serves_a_confirmed_retry(tmp_path: Path) -> None:
    config_repo = tmp_path / "config-repo"
    (config_repo / "config").mkdir(parents=True)
    (config_repo / "config" / "projects.yaml").write_text(PROJECTS_YAML)
    (config_repo / "config" / "policies.yaml").write_text("")
    # serve-console resolves this exact file as its playbook path.
    (config_repo / "config" / "playbooks.yaml").write_text("")
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    database_path = state_dir / "state.db"
    # Pre-seed the retryable issue through the REAL services on the
    # pre-migrated database before the child opens it.
    database = Database.open(database_path)
    try:
        events = EventStore(database)
        queue = QueueService(database, events, GITHUB_REPOS.keys())
        queue.admit(
            AdmissionRequest(
                issue_id=RETRYABLE_CHILD_ISSUE_ID,
                project_key="demo",
                linear_priority=2,
                admitted_by="operator",
                instruction_id="operator-instruction-child-1",
            )
        )
        queue.transition(
            RETRYABLE_CHILD_ISSUE_ID,
            IssueState.BLOCKED,
            actor="operator",
            reason="dependency wait",
        )
    finally:
        database.close()
    port = _free_loopback_port()
    stderr_path = tmp_path / "wrapper-child.log"
    with stderr_path.open("wb") as sink:
        process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                CHILD_WRAPPER,
                str(config_repo),
                str(state_dir),
                str(port),
            ],
            stdout=subprocess.PIPE,
            stderr=sink,
            text=True,
        )
    try:
        assert process.stdout is not None
        credential = _first_line(process.stdout, timeout=30)

        def diagnostics() -> str:
            return stderr_path.read_text(errors="replace")[-2000:]

        assert credential, f"child never printed a credential: {diagnostics()}"
        assert _wait_healthy(port, 30), (
            f"child console never served /healthz: {diagnostics()}"
        )
        with httpx.Client(
            base_url=f"http://127.0.0.1:{port}",
            headers={"user-agent": USER_AGENT},
            follow_redirects=False,
        ) as client:
            _login(client, credential)
            # The rendered RETRY flow, exactly as a phone browser drives it:
            # form -> prepare -> typed-phrase confirm.
            form = client.get("/console/actions/retry")
            assert form.status_code == 200, diagnostics()
            prepared = client.post(
                "/console/actions/retry/prepare",
                data={
                    "csrf": _hidden(form.text, "csrf"),
                    "target": RETRYABLE_CHILD_ISSUE_ID,
                },
            )
            assert prepared.status_code == 200, diagnostics()
            phrase = re.search(r"<strong>([^<]+)</strong>", prepared.text)
            assert phrase is not None
            assert phrase.group(1) == (
                f"RETRY {RETRYABLE_CHILD_ISSUE_ID.upper()}"
            )
            confirmed = client.post(
                "/console/actions/retry/confirm",
                data={
                    "csrf": _hidden(prepared.text, "csrf"),
                    "confirmation_id": _hidden(
                        prepared.text, "confirmation_id"
                    ),
                    "idempotency_key": _hidden(
                        prepared.text, "idempotency_key"
                    ),
                    "confirmation_phrase": phrase.group(1),
                },
            )
            assert confirmed.status_code == 200, diagnostics()
            assert "accepted" in confirmed.text
    finally:
        if process.poll() is None:
            process.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            process.wait(timeout=10)
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)
        if process.stdout is not None:
            process.stdout.close()
    # The durable effect is visible from the parent over its own read-only
    # connection: the child's confirmed retry requeued the blocked issue.
    row = _query_one(
        database_path,
        "SELECT state FROM admitted_issues WHERE issue_id = ?",
        (RETRYABLE_CHILD_ISSUE_ID,),
    )
    assert row is not None
    assert row[0] == "queued"


def test_serve_console_refuses_a_non_loopback_host() -> None:
    binary = Path(sys.executable).parent / "hermes-orchestrator"
    completed = subprocess.run(
        [str(binary), "serve-console", "--host", "0.0.0.0", "--port", "8990"],
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert completed.returncode != 0
