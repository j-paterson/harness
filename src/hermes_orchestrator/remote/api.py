"""Loopback-only authenticated operations API (INFRA-175).

``create_operations_app`` builds the FastAPI application for the remote
operations console. The app refuses at construction time to serve on any
bind host other than 127.0.0.1, attaches the mandated security headers to
every response, authenticates every route against the INFRA-173 session
service, requires a CSRF proof on every mutation, and renders reads only
through the sanitized INFRA-174 view models. Error responses are short
static codes — no exception text, input echo, or internal state ever
reaches a response body.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from fastapi import FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from hermes_orchestrator.remote.auth import (
    SESSION_COOKIE_NAME,
    CsrfService,
    LoginRateLimiter,
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
    UnknownConfirmation,
    UnknownTarget,
    UnsupportedIntent,
)
from hermes_orchestrator.remote.console import build_console_router
from hermes_orchestrator.remote.policy import RemoteIntent, RemotePolicy
from hermes_orchestrator.remote.views import OperationsSummary, RedactionError

LOOPBACK_HOST = "127.0.0.1"

_SECURITY_HEADERS = {
    "Cache-Control": "no-store",
    "Content-Security-Policy": (
        "default-src 'self'; frame-ancestors 'none'; form-action 'self'"
    ),
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
    "Permissions-Policy": "camera=(), microphone=(), geolocation=(), payment=()",
}


class BindHostNotLoopback(ValueError):
    """The configured bind host is not the loopback address."""


class _ApiError(Exception):
    """A short static error code with its HTTP status; never a value."""

    def __init__(self, status_code: int, code: str) -> None:
        super().__init__(code)
        self.status_code = status_code
        self.code = code


# Service failures map to static codes; the exception messages themselves
# are never rendered.
_SERVICE_ERRORS: tuple[tuple[type[Exception], int, str], ...] = (
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


class StatusProvider(Protocol):
    """The sanitized read surface; StatusViewService satisfies this."""

    def summary(self) -> OperationsSummary: ...


@dataclass(frozen=True)
class RemoteDependencies:
    """Everything the operations app needs, injected by the wiring."""

    sessions: SessionService
    csrf: CsrfService
    policy: RemotePolicy
    status: StatusProvider
    confirmations: ConfirmationService
    credentials: RemoteCredentialService
    login_limiter: LoginRateLimiter
    bind_host: str = LOOPBACK_HOST


class _PrepareRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    intent: str
    target: str
    parameters: dict[str, Any] = Field(default_factory=dict)


class _ConfirmRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    confirmation_id: str
    confirmation_phrase: str
    idempotency_key: str


def _fingerprint(request: Request) -> str:
    host = request.client.host if request.client else ""
    return f"{host}|{request.headers.get('user-agent', '')}"


def create_operations_app(dependencies: RemoteDependencies) -> FastAPI:
    """Build the loopback-only operations application."""

    if dependencies.bind_host != LOOPBACK_HOST:
        raise BindHostNotLoopback(
            "the operations console only ever binds the loopback address"
        )

    app = FastAPI(openapi_url=None, docs_url=None, redoc_url=None)

    def _authenticate(request: Request) -> SessionClaims:
        cookie = request.cookies.get(SESSION_COOKIE_NAME)
        if not cookie:
            raise _ApiError(401, "unauthorized")
        try:
            return dependencies.sessions.verify(cookie, _fingerprint(request))
        except RemoteAuthError as error:
            raise _ApiError(401, "unauthorized") from error

    def _require_csrf(request: Request, claims: SessionClaims) -> None:
        presented = request.headers.get("x-csrf-token", "")
        if not presented or not dependencies.csrf.verify(
            claims.session_id, presented
        ):
            raise _ApiError(403, "csrf_rejected")

    @app.middleware("http")
    async def _security_headers(request: Request, call_next) -> Response:
        response = await call_next(request)
        for header, value in _SECURITY_HEADERS.items():
            response.headers[header] = value
        return response

    @app.exception_handler(_ApiError)
    async def _api_error(request: Request, error: _ApiError) -> JSONResponse:
        return JSONResponse(
            status_code=error.status_code, content={"code": error.code}
        )

    @app.exception_handler(RequestValidationError)
    async def _validation_error(
        request: Request, error: RequestValidationError
    ) -> JSONResponse:
        # FastAPI's default 422 body echoes the offending input; this
        # handler replaces it with a static code.
        return JSONResponse(status_code=400, content={"code": "invalid_request"})

    def _service_error_handler(status_code: int, code: str):
        async def handler(request: Request, error: Exception) -> JSONResponse:
            return JSONResponse(status_code=status_code, content={"code": code})

        return handler

    for exception_type, status_code, code in _SERVICE_ERRORS:
        app.add_exception_handler(
            exception_type, _service_error_handler(status_code, code)
        )

    @app.get("/api/status")
    async def status(request: Request) -> dict[str, Any]:
        _authenticate(request)
        decision = dependencies.policy.authorize(RemoteIntent.STATUS)
        if not decision.allowed:
            raise _ApiError(403, "forbidden_intent")
        return dependencies.status.summary().model_dump(mode="json")

    @app.post("/api/commands/prepare")
    async def prepare(request: Request, body: _PrepareRequest) -> dict[str, Any]:
        claims = _authenticate(request)
        _require_csrf(request, claims)
        try:
            intent = RemoteIntent(body.intent)
        except ValueError:
            raise _ApiError(400, "invalid_intent") from None
        pending = dependencies.confirmations.prepare(
            RemoteCommand(
                intent=intent,
                target=body.target,
                parameters=body.parameters,
            ),
            session_id=claims.session_id,
        )
        return {
            "confirmation_id": pending.confirmation_id,
            "confirmation_phrase": pending.confirmation_phrase,
            "impact_summary": pending.impact_summary,
            "expires_at": pending.expires_at.isoformat(),
        }

    @app.post("/api/commands/confirm")
    async def confirm(request: Request, body: _ConfirmRequest) -> dict[str, Any]:
        claims = _authenticate(request)
        _require_csrf(request, claims)
        result = dependencies.confirmations.confirm(
            body.confirmation_id,
            body.confirmation_phrase,
            body.idempotency_key,
            session_id=claims.session_id,
        )
        return {
            "code": result.code,
            "correlation_id": result.correlation_id,
            "state": result.state,
        }

    app.include_router(
        build_console_router(
            sessions=dependencies.sessions,
            csrf=dependencies.csrf,
            policy=dependencies.policy,
            status=dependencies.status,
            confirmations=dependencies.confirmations,
            credentials=dependencies.credentials,
            login_limiter=dependencies.login_limiter,
        )
    )

    return app
