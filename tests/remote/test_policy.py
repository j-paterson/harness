"""Deny-by-default remote authorization over a closed intent enum (INFRA-174)."""

from __future__ import annotations

import dataclasses

import pytest

from hermes_orchestrator.remote.policy import (
    ALLOWED_INTENTS,
    FORBIDDEN_INTENTS,
    AuthorizationDecision,
    RemoteIntent,
    RemotePolicy,
)


@pytest.fixture
def policy() -> RemotePolicy:
    return RemotePolicy()


@pytest.mark.parametrize(
    "intent",
    [
        "status",
        "queue_issue",
        "pause",
        "resume",
        "retry",
        "reprioritize",
        "approve_stall",
        "approve_handoff",
        "request_checkpoint",
        "request_cleanup",
    ],
)
def test_allowed_intents(policy: RemotePolicy, intent: str) -> None:
    decision = policy.authorize(RemoteIntent(intent))
    assert decision.allowed
    assert decision.intent == RemoteIntent(intent)
    assert decision.reason


@pytest.mark.parametrize(
    "intent",
    [
        "shell",
        "read_env",
        "credentials",
        "mcp_admin",
        "provider_auth",
        "spend",
        "force_delete",
        "disable_safety",
    ],
)
def test_forbidden_intents(policy: RemotePolicy, intent: str) -> None:
    decision = policy.authorize(RemoteIntent(intent))
    assert not decision.allowed
    assert decision.reason


@pytest.mark.parametrize(
    "value",
    [
        "reboot",
        "read_environment",
        "sudo",
        "exec",
        "queue-issue",
        "STATUS",
        "status ",
        " status",
        "",
        "restart_worker",
        "export_state",
    ],
)
def test_unknown_intents_fail_parsing(value: str) -> None:
    with pytest.raises(ValueError):
        RemoteIntent(value)


def test_authorize_denies_raw_unknown_string(policy: RemotePolicy) -> None:
    # Structural default-deny: even bypassing the parse step, a value outside
    # the allowlist is denied rather than raising or slipping through.
    decision = policy.authorize("rm -rf /")  # type: ignore[arg-type]
    assert not decision.allowed


def test_enum_is_closed_and_partitioned() -> None:
    assert ALLOWED_INTENTS.isdisjoint(FORBIDDEN_INTENTS)
    assert frozenset(RemoteIntent) == ALLOWED_INTENTS | FORBIDDEN_INTENTS
    assert len(ALLOWED_INTENTS) == 10
    assert len(FORBIDDEN_INTENTS) == 8


def test_every_non_allowlisted_member_is_denied(policy: RemotePolicy) -> None:
    for member in RemoteIntent:
        decision = policy.authorize(member)
        assert decision.allowed == (member in ALLOWED_INTENTS)


def test_decision_shape_is_exact() -> None:
    names = {field.name for field in dataclasses.fields(AuthorizationDecision)}
    assert names == {"allowed", "intent", "reason"}


def test_decision_is_frozen() -> None:
    decision = AuthorizationDecision(
        allowed=False,
        intent=RemoteIntent("shell"),
        reason="intent is not allowlisted",
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        decision.allowed = True  # type: ignore[misc]
