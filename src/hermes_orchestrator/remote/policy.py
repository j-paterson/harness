"""Deny-by-default remote authorization over a closed intent enum (INFRA-174).

The phone surface may only ever request intents that exist in
:class:`RemoteIntent`; any other string fails parsing, so intents that do not
exist yet are unrepresentable. Authorization then grants solely on membership
in :data:`ALLOWED_INTENTS` — there is no blocklist branch, so a member absent
from the allowlist (including the explicitly forbidden ones) is denied by
default.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class RemoteIntent(StrEnum):
    """Every intent the remote surface can even name.

    Unknown values raise ``ValueError`` at parse time. The forbidden members
    exist so operator requests for them parse and receive an explicit denial
    rather than an unparseable-input error, but they carry no privilege: the
    only grant path is the allowlist below.
    """

    STATUS = "status"
    QUEUE_ISSUE = "queue_issue"
    PAUSE = "pause"
    RESUME = "resume"
    RETRY = "retry"
    REPRIORITIZE = "reprioritize"
    APPROVE_STALL = "approve_stall"
    APPROVE_HANDOFF = "approve_handoff"
    REQUEST_CHECKPOINT = "request_checkpoint"
    REQUEST_CLEANUP = "request_cleanup"
    SHELL = "shell"
    READ_ENV = "read_env"
    CREDENTIALS = "credentials"
    MCP_ADMIN = "mcp_admin"
    PROVIDER_AUTH = "provider_auth"
    SPEND = "spend"
    FORCE_DELETE = "force_delete"
    DISABLE_SAFETY = "disable_safety"


ALLOWED_INTENTS: frozenset[RemoteIntent] = frozenset(
    {
        RemoteIntent.STATUS,
        RemoteIntent.QUEUE_ISSUE,
        RemoteIntent.PAUSE,
        RemoteIntent.RESUME,
        RemoteIntent.RETRY,
        RemoteIntent.REPRIORITIZE,
        RemoteIntent.APPROVE_STALL,
        RemoteIntent.APPROVE_HANDOFF,
        RemoteIntent.REQUEST_CHECKPOINT,
        RemoteIntent.REQUEST_CLEANUP,
    }
)

FORBIDDEN_INTENTS: frozenset[RemoteIntent] = frozenset(RemoteIntent) - ALLOWED_INTENTS


@dataclass(frozen=True, slots=True)
class AuthorizationDecision:
    """The outcome of one authorization check."""

    allowed: bool
    intent: RemoteIntent
    reason: str


class RemotePolicy:
    """Grant on allowlist membership only; everything else is denied."""

    def authorize(self, intent: RemoteIntent) -> AuthorizationDecision:
        if intent in ALLOWED_INTENTS:
            return AuthorizationDecision(
                allowed=True,
                intent=intent,
                reason="intent is allowlisted for remote operation",
            )
        return AuthorizationDecision(
            allowed=False,
            intent=intent,
            reason="intent is not allowlisted for remote operation",
        )
