"""Server-rendered mobile-first operations console (INFRA-176).

A thin HTML presentation layer over capabilities that already exist: reads
render the INFRA-174 sanitized views, and every mutation flows through the
INFRA-175 ``ConfirmationService`` prepare -> typed-phrase confirm path with
CSRF, idempotency keys, replay protection, and audits — the console adds no
intent, no policy grant, and no mutation path of its own. Plain HTML forms
carry the INFRA-173 CSRF token as a hidden field, so every flow works
without JavaScript. Templates render only sanitized view fields and static
codes; no caller input, exception text, or executor state ever reaches a
page.
"""

from __future__ import annotations

import secrets
from dataclasses import dataclass
from importlib import resources
from typing import Protocol

from fastapi import APIRouter, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from jinja2 import Environment, PackageLoader
from starlette.datastructures import FormData

from hermes_orchestrator.remote.auth import (
    SESSION_COOKIE_NAME,
    CsrfService,
    LoginRateLimiter,
    RateLimited,
    RemoteAuthError,
    RemoteCredentialService,
    SessionClaims,
    SessionService,
)
from hermes_orchestrator.remote.commands import (
    ConfirmationExpired,
    ConfirmationReplayed,
    ConfirmationService,
    ExecutionUnresolved,
    IdempotencyKeyConflict,
    IncompatibleTarget,
    IntentDenied,
    InvalidParameters,
    PhraseMismatch,
    RemoteCommand,
    RemoteCommandError,
    UnknownConfirmation,
    UnknownTarget,
    UnsupportedIntent,
)
from hermes_orchestrator.remote.policy import RemoteIntent, RemotePolicy
from hermes_orchestrator.remote.views import OperationsSummary, RedactionError


@dataclass(frozen=True, slots=True)
class ConsoleField:
    """One labeled operator input on an action form."""

    name: str
    label: str
    input_type: str = "text"


@dataclass(frozen=True, slots=True)
class ConsoleAction:
    """One console action bound to an existing allowlisted intent."""

    slug: str
    title: str
    intent: RemoteIntent
    target_kind: str
    target_label: str
    fields: tuple[ConsoleField, ...] = ()


_PRIORITY = ConsoleField("priority", "Priority (1-4)", "number")

# The closed table of console actions. Every entry maps onto an intent that
# is already allowlisted and specced; the console can never name anything
# else, so a new action here without an existing capability fails loudly in
# prepare rather than widening the surface.
CONSOLE_ACTIONS: dict[str, ConsoleAction] = {
    action.slug: action
    for action in (
        ConsoleAction(
            "pause", "Pause project", RemoteIntent.PAUSE, "project", "Project"
        ),
        ConsoleAction(
            "resume", "Resume project", RemoteIntent.RESUME, "project", "Project"
        ),
        ConsoleAction("retry", "Retry issue", RemoteIntent.RETRY, "issue", "Issue"),
        ConsoleAction(
            "reprioritize",
            "Reprioritize issue",
            RemoteIntent.REPRIORITIZE,
            "issue",
            "Issue",
            (_PRIORITY,),
        ),
        ConsoleAction(
            "checkpoint",
            "Request checkpoint",
            RemoteIntent.REQUEST_CHECKPOINT,
            "project",
            "Project",
        ),
        ConsoleAction(
            "cleanup",
            "Request cleanup",
            RemoteIntent.REQUEST_CLEANUP,
            "project",
            "Project",
        ),
        ConsoleAction(
            "approve-stall",
            "Approve stall",
            RemoteIntent.APPROVE_STALL,
            "project",
            "Project",
        ),
        ConsoleAction(
            "approve-handoff",
            "Approve handoff",
            RemoteIntent.APPROVE_HANDOFF,
            "handoff",
            "Handoff",
        ),
        ConsoleAction(
            "queue",
            "Queue issue",
            RemoteIntent.QUEUE_ISSUE,
            "project",
            "Project",
            (ConsoleField("issue_id", "Issue"), _PRIORITY),
        ),
    )
}

_CONFIRM_FORM_KEYS = frozenset(
    {"confirmation_id", "confirmation_phrase", "idempotency_key", "csrf"}
)

# Service failures render as the same static codes and statuses the JSON
# surface maps in api.py; the exception messages themselves never render.
_SERVICE_ERROR_PAGES: tuple[tuple[type[Exception], int, str], ...] = (
    (UnknownTarget, 404, "unknown_target"),
    (UnknownConfirmation, 404, "unknown_confirmation"),
    (PhraseMismatch, 409, "phrase_mismatch"),
    (ConfirmationExpired, 410, "confirmation_expired"),
    (ConfirmationReplayed, 409, "confirmation_replayed"),
    (IntentDenied, 403, "forbidden_intent"),
    (UnsupportedIntent, 400, "unsupported_intent"),
    (IncompatibleTarget, 400, "incompatible_target"),
    (InvalidParameters, 400, "invalid_parameters"),
    (ExecutionUnresolved, 409, "execution_unresolved"),
    (IdempotencyKeyConflict, 409, "idempotency_key_conflict"),
    (RedactionError, 500, "redaction_failure"),
)

_env = Environment(
    loader=PackageLoader("hermes_orchestrator.remote", "templates"),
    autoescape=True,
)

# The production CSP is default-src 'self' with no unsafe-inline, so all
# styling ships as this packaged same-origin stylesheet.
_STYLESHEET = (
    resources.files("hermes_orchestrator.remote")
    .joinpath("static/console.css")
    .read_text()
)

# The clearing counterpart of SessionCookie.header_value(): identical
# hardened attributes, immediate expiry.
_CLEARED_SESSION_COOKIE = (
    f"{SESSION_COOKIE_NAME}=; HttpOnly; Secure; SameSite=Strict; "
    "Path=/; Max-Age=0"
)


class StatusProvider(Protocol):
    """The sanitized read surface; StatusViewService satisfies this."""

    def summary(self) -> OperationsSummary: ...


class _PageError(Exception):
    """A console-level failure rendered as a static code, never a value."""

    def __init__(self, status_code: int, code: str) -> None:
        super().__init__(code)
        self.status_code = status_code
        self.code = code


def _render(template: str, status_code: int = 200, **context: object) -> HTMLResponse:
    page = _env.get_template(template).render(**context)
    return HTMLResponse(page, status_code=status_code)


def _error(status_code: int, code: str) -> HTMLResponse:
    return _render("error.html", status_code=status_code, code=code)


def _service_error(error: Exception) -> HTMLResponse:
    for exception_type, status_code, code in _SERVICE_ERROR_PAGES:
        if isinstance(error, exception_type):
            return _error(status_code, code)
    raise error


def _fingerprint(request: Request) -> str:
    host = request.client.host if request.client else ""
    return f"{host}|{request.headers.get('user-agent', '')}"


def build_console_router(
    *,
    sessions: SessionService,
    csrf: CsrfService,
    policy: RemotePolicy,
    status: StatusProvider,
    confirmations: ConfirmationService,
    credentials: RemoteCredentialService,
    login_limiter: LoginRateLimiter,
) -> APIRouter:
    """Build the console routes over the injected existing services."""

    router = APIRouter()

    def _authenticate(request: Request) -> SessionClaims:
        cookie = request.cookies.get(SESSION_COOKIE_NAME)
        if not cookie:
            raise _PageError(401, "unauthorized")
        try:
            return sessions.verify(cookie, _fingerprint(request))
        except RemoteAuthError as error:
            raise _PageError(401, "unauthorized") from error

    def _require_csrf(claims: SessionClaims, form: FormData) -> None:
        presented = str(form.get("csrf") or "")
        if not presented or not csrf.verify(claims.session_id, presented):
            raise _PageError(403, "csrf_rejected")

    def _action(slug: str) -> ConsoleAction:
        action = CONSOLE_ACTIONS.get(slug)
        # An action whose intent the wired executor cannot serve is treated
        # exactly like an unknown one: it must not render a form that could
        # only fail at prepare. GET and POST routes all resolve through
        # this lookup, so the removal is total.
        if action is None or not confirmations.available(action.intent):
            raise _PageError(404, "unknown_action")
        return action

    @router.get("/static/console.css")
    async def console_stylesheet() -> Response:
        # Anonymous by design: the stylesheet is static presentation with
        # no data, and the login page needs it before any session exists.
        return Response(_STYLESHEET, media_type="text/css")

    @router.get("/login", response_class=HTMLResponse)
    async def login_form(request: Request) -> Response:
        cookie = request.cookies.get(SESSION_COOKIE_NAME)
        if cookie:
            try:
                sessions.verify(cookie, _fingerprint(request))
            except RemoteAuthError:
                pass
            else:
                return RedirectResponse("/console", status_code=303)
        return _render("login.html")

    @router.post("/login", response_class=HTMLResponse)
    async def login_submit(request: Request) -> Response:
        form = await request.form()
        if set(form.keys()) != {"passcode"}:
            return _error(400, "invalid_request")
        address = request.client.host if request.client else ""
        fingerprint = _fingerprint(request)
        # The limiter is consulted before any verification so a locked-out
        # client learns nothing about the submitted value.
        try:
            login_limiter.check(address, fingerprint)
        except RateLimited:
            return _error(429, "rate_limited")
        if not credentials.verify(str(form["passcode"])):
            login_limiter.record_failure(address, fingerprint)
            return _render("login.html", status_code=401, failed=True)
        login_limiter.record_success(address, fingerprint)
        cookie = sessions.create(fingerprint)
        # The redirect target is a fixed internal path, never caller input.
        response = RedirectResponse("/console", status_code=303)
        response.headers.append("set-cookie", cookie.header_value())
        return response

    @router.post("/logout", response_class=HTMLResponse)
    async def logout(request: Request) -> Response:
        try:
            claims = _authenticate(request)
            form = await request.form()
            _require_csrf(claims, form)
            if set(form.keys()) != {"csrf"}:
                raise _PageError(400, "invalid_request")
        except _PageError as error:
            return _error(error.status_code, error.code)
        sessions.revoke(claims.session_id)
        response = RedirectResponse("/login", status_code=303)
        response.headers.append("set-cookie", _CLEARED_SESSION_COOKIE)
        return response

    @router.get("/", response_class=HTMLResponse)
    async def root(request: Request) -> Response:
        # Opening the service origin routes by session state alone. Both
        # redirect targets are fixed internal literals, and the anonymous
        # branch renders nothing, so no summary data exists before auth.
        cookie = request.cookies.get(SESSION_COOKIE_NAME)
        if cookie:
            try:
                sessions.verify(cookie, _fingerprint(request))
            except RemoteAuthError:
                pass
            else:
                return RedirectResponse("/console", status_code=303)
        return RedirectResponse("/login", status_code=303)

    @router.get("/projects/{project_key}", response_class=HTMLResponse)
    async def console_project(request: Request, project_key: str) -> HTMLResponse:
        try:
            _authenticate(request)
            if not policy.authorize(RemoteIntent.STATUS).allowed:
                raise _PageError(403, "forbidden_intent")
            summary = status.summary()
        except _PageError as error:
            return _error(error.status_code, error.code)
        except RedactionError as error:
            return _service_error(error)
        project = next(
            (
                item
                for item in summary.projects
                if item.project_alias == project_key
            ),
            None,
        )
        if project is None:
            # Static code only: the requested key never renders.
            return _error(404, "unknown_project")
        # Every rendered value comes from the sanitized view rows filtered
        # by the matched alias, never from the path parameter itself.
        alias = project.project_alias
        return _render(
            "project.html",
            project=project,
            workers=[i for i in summary.workers if i.project_alias == alias],
            queue=[i for i in summary.queue if i.project_alias == alias],
            reviews=[i for i in summary.reviews if i.project_alias == alias],
            stalls=[i for i in summary.stalls if i.project_alias == alias],
        )

    @router.get("/console", response_class=HTMLResponse)
    async def console_status(request: Request) -> HTMLResponse:
        try:
            claims = _authenticate(request)
            if not policy.authorize(RemoteIntent.STATUS).allowed:
                raise _PageError(403, "forbidden_intent")
            summary = status.summary()
        except _PageError as error:
            return _error(error.status_code, error.code)
        except RedactionError as error:
            return _service_error(error)
        return _render(
            "status.html",
            summary=summary,
            # Only capabilities the wired executor actually serves render;
            # the closed table itself stays intact as the naming boundary.
            actions=[
                action
                for action in CONSOLE_ACTIONS.values()
                if confirmations.available(action.intent)
            ],
            csrf=csrf.issue(claims.session_id),
        )

    @router.get("/console/actions/{slug}", response_class=HTMLResponse)
    async def console_action_form(request: Request, slug: str) -> HTMLResponse:
        try:
            claims = _authenticate(request)
            action = _action(slug)
        except _PageError as error:
            return _error(error.status_code, error.code)
        return _render(
            "action_form.html", action=action, csrf=csrf.issue(claims.session_id)
        )

    @router.post("/console/actions/{slug}/prepare", response_class=HTMLResponse)
    async def console_prepare(request: Request, slug: str) -> HTMLResponse:
        try:
            claims = _authenticate(request)
            action = _action(slug)
            form = await request.form()
            _require_csrf(claims, form)
            allowed = {"target", "csrf"} | {field.name for field in action.fields}
            if set(form.keys()) != allowed:
                raise _PageError(400, "invalid_request")
            parameters = {
                field.name: str(form[field.name]) for field in action.fields
            }
            if action.intent is RemoteIntent.QUEUE_ISSUE:
                # The operator never chooses the instruction identity: it is
                # minted once per prepare and persisted with the stored
                # parameters, so an idempotent retry keeps the original
                # identity while distinct submissions get distinct values,
                # each bound to the preparing session.
                parameters["operator_instruction_id"] = (
                    f"console-{claims.session_id}-{secrets.token_urlsafe(16)}"
                )
            pending = confirmations.prepare(
                RemoteCommand(
                    intent=action.intent,
                    target=f"{action.target_kind}:{form['target']}",
                    parameters=parameters,
                ),
                session_id=claims.session_id,
            )
        except _PageError as error:
            return _error(error.status_code, error.code)
        except (RemoteCommandError, RedactionError) as error:
            return _service_error(error)
        # The idempotency key is minted once per rendered confirm form, so a
        # double-tapped submit retries the same key and executes exactly once.
        return _render(
            "confirm.html",
            action=action,
            phrase=pending.confirmation_phrase,
            impact=pending.impact_summary,
            expires_at=pending.expires_at.isoformat(),
            confirmation_id=pending.confirmation_id,
            idempotency_key=secrets.token_urlsafe(16),
            csrf=csrf.issue(claims.session_id),
        )

    @router.post("/console/actions/{slug}/confirm", response_class=HTMLResponse)
    async def console_confirm(request: Request, slug: str) -> HTMLResponse:
        try:
            claims = _authenticate(request)
            _action(slug)
            form = await request.form()
            _require_csrf(claims, form)
            if set(form.keys()) != _CONFIRM_FORM_KEYS:
                raise _PageError(400, "invalid_request")
            result = confirmations.confirm(
                str(form["confirmation_id"]),
                str(form["confirmation_phrase"]),
                str(form["idempotency_key"]),
                session_id=claims.session_id,
            )
        except _PageError as error:
            return _error(error.status_code, error.code)
        except (RemoteCommandError, RedactionError) as error:
            return _service_error(error)
        # Executor state values never render; the page carries only the
        # static result code and the correlation id.
        return _render(
            "result.html", code=result.code, correlation_id=result.correlation_id
        )

    return router
