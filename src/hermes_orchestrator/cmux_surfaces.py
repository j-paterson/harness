"""Durable cmux surface bindings, reconciliation, and hibernation gating.

INFRA-185: Hermes binds each orchestration seat — the Orchestrator pane
and one workspace per active Claude project lead — to exact cmux
workspace/surface UUIDs in durable state. Every lifecycle transition
(bound, replaced, closed, lost) journals a replay-safe event; ownership
never transfers silently and reconciliation validates the exact live
surface rather than adopting terminals by cwd or title. Terminal
visibility stays observational: nothing in this module reads screens or
advances issue state, and hibernation clearance derives only from durable
cell, lease, and checkpoint-safety evidence.
"""

from __future__ import annotations

import asyncio
import re
import subprocess
import threading
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import psutil

from hermes_orchestrator.channel_trust import (
    ChannelTrustAnchors,
    ChannelTrustGate,
    TrustVerdict,
)
from hermes_orchestrator.cmux import (
    CmuxControlPort,
    CmuxError,
    CmuxSurfaceRef,
)
from hermes_orchestrator.control_operations import ControlOperations
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore
from hermes_orchestrator.worktrees import CleanupBlocked, WorktreeLeases

CMUX_WORKSPACE_ID_ENV = "CMUX_WORKSPACE_ID"
CMUX_SURFACE_ID_ENV = "CMUX_SURFACE_ID"

ORCHESTRATOR_TITLE = "Orchestrator"

_RUNNING_LEASE_STATES = ("active", "stopping")

# Which prior lifecycle state may enter each target state. 'residual'
# holds ownership evidence for a workspace that is not (or not yet) the
# active seat: a write-ahead activation or an unconfirmed close. It
# resolves forward to active (promotion), closed, or lost only.
_TRANSITION_SOURCES: Mapping[str, tuple[str, ...]] = {
    "active": ("residual",),
    "stale": ("active",),
    "residual": ("active",),
    "closed": ("active", "residual"),
    "lost": ("active", "residual"),
}


class CmuxBindingConflict(RuntimeError):
    """A live binding already owns this seat; replacement must be explicit."""


class SeatAuthRefused(RuntimeError):
    """The leased profile's auth probe did not prove the intended
    first-party Max account; no seat is created and no lead may start."""


# The complete grammar of a classic in-pane lead command: the native
# interactive Claude TUI addressing exactly one session, optionally extended
# by the controller-resolved project-lead prompt — never caller-supplied
# prompt text, flag soup, or a credential. Immediately after the session UUID
# sits the fixed ``--dangerously-skip-permissions`` literal (INFRA-197
# operator decision infra-197-managed-claude-skip-permissions-20260830-
# v1: every Hermes-managed classic launch carries it) — it is not
# caller-supplied, sits before any channel extension so every extended
# command inherits it by construction, and no grammar position lets a
# caller express a different or additional flag there. Exactly two
# extensions are permitted after it, mutually exclusive: the
# controller-generated session-scoped MCP config plus the fixed
# hermes-control development-channel entry — the one production
# extension (Sol correction b4b545f3, v5) — or the fixed official
# fakechat channel plugin literal, retained in the grammar only so
# historical v4 commands still validate-or-refuse exactly; no
# production path composes the fakechat form any more. Arbitrary
# flags, commands, and user-selected paths remain structurally
# impossible.
_UUID_PATTERN = (
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
CHANNEL_ENTRY = "server:hermes-control"
FAKECHAT_CHANNEL_ENTRY = "plugin:fakechat@claude-plugins-official"
SKIP_PERMISSIONS_FLAG = "--dangerously-skip-permissions"
_CLASSIC_COMMAND = re.compile(
    r"^claude --(resume|session-id) " + _UUID_PATTERN
    + " " + re.escape(SKIP_PERMISSIONS_FLAG)
    + r"( --append-system-prompt-file /[A-Za-z0-9._/-]+/prompts/"
    r"claude-(lead|harness)\.md)?("
    r" --mcp-config /[A-Za-z0-9._/-]+/" + _UUID_PATTERN + r"\.mcp\.json"
    r" --dangerously-load-development-channels " + re.escape(CHANNEL_ENTRY)
    + r"| --channels " + re.escape(FAKECHAT_CHANNEL_ENTRY)
    + r")?$"
)
# The config path may hold nothing a shell, cmux, or argv parser could
# reinterpret — no spaces, quotes, or metacharacters.
_CHANNEL_CONFIG_PATH = re.compile(r"^/[A-Za-z0-9._/-]+$")
_LEAD_PROMPT_PATH = re.compile(
    r"^/[A-Za-z0-9._/-]+/prompts/claude-(lead|harness)\.md$"
)


def classic_resume_command(
    session_id: str, *, resume: bool, prompt_file: Path | None = None
) -> str:
    """The sanitized native command that runs the classic TUI in-pane.

    ``--resume`` reattaches an existing session; ``--session-id`` starts
    a new one under the exact preassigned identity. The session id must
    parse as a UUID, so nothing else can ever ride along. The fixed
    ``--dangerously-skip-permissions`` literal (INFRA-197 operator
    decision infra-197-managed-claude-skip-permissions-20260830-v1)
    always follows the UUID, before any channel extension, so every
    Hermes-managed classic launch — and every command built on top of
    this one — carries it by construction; it is never caller-supplied.
    """

    canonical = str(uuid.UUID(str(session_id)))
    flag = "--resume" if resume else "--session-id"
    command = f"claude {flag} {canonical} {SKIP_PERMISSIONS_FLAG}"
    if prompt_file is None:
        return command
    path = str(prompt_file)
    if _LEAD_PROMPT_PATH.fullmatch(path) is None:
        raise CmuxBindingConflict(
            "only a resolved Claude lead prompt may extend a classic seat"
        )
    return f"{command} --append-system-prompt-file {path}"


def classic_channel_command(
    session_id: str,
    *,
    resume: bool,
    channel_config: Path,
    prompt_file: Path | None = None,
) -> str:
    """The classic command extended with exactly one channel load.

    Accepts only the controller-generated session config — an absolute
    metacharacter-free path whose filename is exactly this session's
    canonical UUID plus ``.mcp.json`` — and the fixed
    ``server:hermes-control`` entry. Anything else refuses before any
    command exists. The base command from :func:`classic_resume_command`
    already carries the fixed ``--dangerously-skip-permissions`` literal
    ahead of this extension.
    """

    base = classic_resume_command(
        session_id, resume=resume, prompt_file=prompt_file
    )
    canonical = str(uuid.UUID(str(session_id)))
    path = str(channel_config)
    if (
        _CHANNEL_CONFIG_PATH.fullmatch(path) is None
        or channel_config.name != f"{canonical}.mcp.json"
    ):
        raise CmuxBindingConflict(
            "only the controller-generated session-scoped config may "
            "load a development channel"
        )
    return (
        f"{base} --mcp-config {path} "
        f"--dangerously-load-development-channels {CHANNEL_ENTRY}"
    )


def classic_fakechat_command(session_id: str, *, resume: bool) -> str:
    """The classic command extended with the fixed fakechat channel.

    Retired from production (Sol correction b4b545f3, v5): no seat
    composition calls this builder any more — the hermes-control
    channel launch in :func:`classic_channel_command` is the one
    production extension. The builder survives, still grammar-exact,
    only so the historical v4 command shape remains constructible for
    validation and tests; nothing about it is caller-supplied beyond
    the session id round-tripping through
    :func:`classic_resume_command`.
    """

    base = classic_resume_command(session_id, resume=resume)
    return f"{base} --channels {FAKECHAT_CHANNEL_ENTRY}"


@dataclass(frozen=True, slots=True)
class CmuxActivationIntent:
    """One write-ahead workspace activation, committed before the create.

    The intent_id is the durable unique operation identity: it travels
    inside the created workspace's title, so an interruption anywhere
    between the external create and the durable UUID bind still leaves
    the exact workspace correlatable to this row and to nothing else.
    """

    intent_id: str
    project_key: str
    cell_id: str
    session_id: str
    profile_alias: str
    # INFRA-219 R5 (Sol correction 110ed759): the lane this activation
    # belongs to ('development' or 'harness'), carried from the intent
    # through to the residual/active binding it produces so restart
    # recovery can resolve the harness lane's OWN dedicated worktree
    # instead of the development lead_cwd.
    lane_role: str
    state: str
    binding_id: str | None
    created_at: str
    updated_at: str

    @property
    def title_marker(self) -> str:
        return f"[hermes:{self.intent_id}]"


@dataclass(frozen=True, slots=True)
class CmuxBinding:
    """One durable seat binding to an exact cmux workspace and surface."""

    binding_id: str
    role: str
    project_key: str | None
    cell_id: str | None
    session_id: str | None
    profile_alias: str | None
    # INFRA-219 R5 (Sol correction 110ed759): which lane this seat
    # belongs to ('development' or 'harness'), defaulting to
    # 'development' at the schema level (migration 0056) so every
    # binding predating this packet is unambiguously the development
    # lane. An 'orchestrator' binding carries the schema default too —
    # it has no lane of its own.
    lane_role: str
    workspace_uuid: str
    surface_uuid: str
    generation: int
    state: str
    created_at: str
    updated_at: str

    @property
    def ref(self) -> CmuxSurfaceRef:
        return CmuxSurfaceRef(
            workspace_uuid=self.workspace_uuid,
            surface_uuid=self.surface_uuid,
        )


class ProfileDirectory(Protocol):
    """Resolve a profile alias to its exact CLAUDE_CONFIG_DIR."""

    def config_dir(self, alias: str) -> Path: ...


class ChannelLaunchSource(Protocol):
    """Generate and retire session-scoped channel launch material."""

    def generate(
        self,
        *,
        project_key: str,
        cell_id: str,
        session_id: str,
        profile_alias: str,
        generation: int,
    ) -> Path: ...

    def cleanup(self, session_id: str) -> None: ...


class CmuxSurfaceBindings:
    """Own the durable seat-to-surface identity map and its lifecycle."""

    def __init__(
        self,
        *,
        database: Database,
        events: EventStore,
        now: Callable[[], datetime] | None = None,
        ids: Callable[[], str] | None = None,
    ) -> None:
        self._database = database
        self._events = events
        self._now = now or (lambda: datetime.now(UTC))
        self._ids = ids or (lambda: uuid.uuid4().hex)

    def bind_orchestrator(self, ref: CmuxSurfaceRef) -> CmuxBinding:
        """Bind the single Orchestrator seat; duplicates reuse the row.

        A second activation against the same live surface returns the
        existing binding unchanged. A different surface while one is
        active is an ownership conflict: the caller must go through
        :meth:`replace` with an explicit reason.
        """

        existing = self.active_orchestrator()
        if existing is not None:
            if existing.ref == ref:
                return existing
            raise CmuxBindingConflict(
                "an active Orchestrator binding already owns another surface"
            )
        return self._insert(
            role="orchestrator",
            project_key=None,
            cell_id=None,
            session_id=None,
            profile_alias=None,
            ref=ref,
            generation=self._next_generation(role="orchestrator"),
            lane_role="development",
        )

    def bind_lead(
        self,
        *,
        project_key: str,
        cell_id: str,
        session_id: str,
        profile_alias: str,
        ref: CmuxSurfaceRef,
        lane_role: str = "development",
    ) -> CmuxBinding:
        """Bind one lead cell's seat; duplicate activation reuses the row.

        INFRA-219 R5 (Sol correction 110ed759): ``lane_role`` defaults
        to ``'development'`` so every existing call site (development
        is the only lane a cell_id has ever carried) behaves exactly as
        before; a harness cell's dedicated ``cell_id`` binds its own
        row carrying ``lane_role='harness'``.
        """

        existing = self.active_lead(cell_id)
        if existing is not None:
            if existing.ref == ref and existing.session_id == session_id:
                return existing
            raise CmuxBindingConflict(
                "an active binding already owns this cell's surface"
            )
        return self._insert(
            role="lead",
            project_key=project_key,
            cell_id=cell_id,
            session_id=session_id,
            profile_alias=profile_alias,
            ref=ref,
            generation=self._next_generation(cell_id=cell_id),
            lane_role=lane_role,
        )

    def replace(
        self, binding_id: str, ref: CmuxSurfaceRef, *, reason: str
    ) -> CmuxBinding:
        """Retire one binding as stale and activate its next generation.

        Both generations commit in one transaction: no failure or
        interruption can leave the old generation terminal without its
        durable successor.
        """

        current = self.get(binding_id)
        if current.state != "active":
            raise CmuxBindingConflict(f"a {current.state} binding cannot be replaced")
        successor_id = self._ids()
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            self._write_transition(
                connection,
                current,
                "stale",
                event="replaced",
                reason=reason,
                stamp=stamp,
            )
            self._write_insert(
                connection,
                successor_id,
                role=current.role,
                project_key=current.project_key,
                cell_id=current.cell_id,
                session_id=current.session_id,
                profile_alias=current.profile_alias,
                ref=ref,
                generation=current.generation + 1,
                state="active",
                event="bound",
                reason=None,
                stamp=stamp,
                lane_role=current.lane_role,
            )
        return self.get(successor_id)

    def activate_residual(
        self,
        binding_id: str,
        *,
        replacing: str | None = None,
        reason: str | None = None,
    ) -> CmuxBinding:
        """Atomically commit a write-ahead residual as the active seat.

        The replaced predecessor (when given) retires in the same
        transaction as the promotion, so an interruption can never leave
        a terminal old generation without a durable successor, and the
        pending row itself was already recoverable ownership evidence.
        """

        pending = self.get(binding_id)
        if pending.state != "residual":
            raise CmuxBindingConflict("only a residual binding can be activated")
        predecessor = None if replacing is None else self.get(replacing)
        if predecessor is not None and predecessor.state != "active":
            raise CmuxBindingConflict(
                f"a {predecessor.state} binding cannot be replaced"
            )
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            if predecessor is not None:
                self._write_transition(
                    connection,
                    predecessor,
                    "stale",
                    event="replaced",
                    reason=reason or "replaced",
                    stamp=stamp,
                )
            self._write_transition(
                connection,
                pending,
                "active",
                event="bound",
                reason=None,
                stamp=stamp,
            )
        return self.get(binding_id)

    def mark_closed(self, binding_id: str, *, reason: str) -> CmuxBinding:
        """Record the seat's explicit terminal closure."""

        return self._transition(
            self.get(binding_id), "closed", event="closed", reason=reason
        )

    def mark_lost(self, binding_id: str, *, reason: str) -> CmuxBinding:
        """Record that the seat's exact surface no longer exists."""

        return self._transition(
            self.get(binding_id), "lost", event="lost", reason=reason
        )

    def mark_residual(self, binding_id: str, *, reason: str) -> CmuxBinding:
        """Hold a seat whose workspace close was never confirmed.

        The binding leaves the active set but keeps its exact workspace
        identity as ownership evidence: reconciliation retries the close
        and hibernation stays blocked until the residue is resolved.
        """

        return self._transition(
            self.get(binding_id), "residual", event="residual", reason=reason
        )

    def record_residual(
        self,
        *,
        project_key: str,
        cell_id: str,
        session_id: str,
        profile_alias: str,
        ref: CmuxSurfaceRef,
        reason: str,
        lane_role: str = "development",
    ) -> CmuxBinding:
        """Persist ownership of a created workspace that never activated.

        Compensating a failed activation may itself fail; this records the
        exact live workspace as a residual binding so startup
        reconciliation can close it instead of leaking an untracked seat.
        """

        return self._insert(
            role="lead",
            project_key=project_key,
            cell_id=cell_id,
            session_id=session_id,
            profile_alias=profile_alias,
            ref=ref,
            generation=self._next_generation(cell_id=cell_id),
            state="residual",
            event="residual",
            reason=reason,
            lane_role=lane_role,
        )

    def record_intent(
        self,
        *,
        project_key: str,
        cell_id: str,
        session_id: str,
        profile_alias: str,
        lane_role: str = "development",
    ) -> CmuxActivationIntent:
        """Commit the activation's durable identity before any external
        create, so no later interruption can orphan the workspace.

        INFRA-219 R5 (Sol correction 110ed759): ``lane_role`` travels
        with the intent so the residual/active binding it eventually
        produces (:meth:`bind_intent`) carries the SAME lane identity —
        never silently defaulting a harness activation's successor
        binding back to development.
        """

        intent_id = self._ids()
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            connection.execute(
                "INSERT INTO cmux_activation_intents("
                "intent_id, project_key, cell_id, session_id, "
                "profile_alias, lane_role, state, binding_id, "
                "created_at, updated_at"
                ") VALUES (?, ?, ?, ?, ?, ?, 'pending', NULL, ?, ?)",
                (
                    intent_id,
                    project_key,
                    cell_id,
                    session_id,
                    profile_alias,
                    lane_role,
                    stamp,
                    stamp,
                ),
            )
            self._events.append(
                connection,
                EventInput(
                    event_type="cmux_intent.recorded",
                    aggregate_type="cmux_intent",
                    aggregate_id=intent_id,
                    payload={
                        "project_key": project_key,
                        "cell_id": cell_id,
                        "session_id": session_id,
                        "profile_alias": profile_alias,
                        "lane_role": lane_role,
                    },
                ),
            )
        return self.get_intent(intent_id)

    def bind_intent(self, intent_id: str, *, ref: CmuxSurfaceRef) -> CmuxBinding:
        """Atomically bind the returned workspace identities to their
        write-ahead intent as a residual (not yet active) seat.

        The binding insert and the intent's pending → bound transition
        commit in one transaction: from this point the binding lifecycle
        owns the workspace and the intent can never resolve again.
        """

        intent = self._pending_intent(intent_id, action="bind a workspace")
        binding_id = self._ids()
        stamp = self._now().isoformat()
        generation = self._next_generation(cell_id=intent.cell_id)
        with self._database.transaction() as connection:
            self._write_insert(
                connection,
                binding_id,
                role="lead",
                project_key=intent.project_key,
                cell_id=intent.cell_id,
                session_id=intent.session_id,
                profile_alias=intent.profile_alias,
                ref=ref,
                generation=generation,
                state="residual",
                event="residual",
                reason="activation_pending",
                stamp=stamp,
                lane_role=intent.lane_role,
            )
            self._write_intent_transition(
                connection,
                intent,
                "bound",
                event="bound",
                binding_id=binding_id,
                stamp=stamp,
                extra={"workspace_uuid": ref.workspace_uuid},
            )
        return self.get(binding_id)

    def abort_intent(self, intent_id: str, *, reason: str) -> CmuxActivationIntent:
        """Record that no workspace ever carried this intent's identity."""

        intent = self._pending_intent(intent_id, action="be aborted")
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            self._write_intent_transition(
                connection,
                intent,
                "aborted",
                event="aborted",
                binding_id=None,
                stamp=stamp,
                extra={"reason": reason},
            )
        return self.get_intent(intent_id)

    def reclaim_intent(
        self,
        intent_id: str,
        *,
        workspace_uuids: tuple[str, ...],
        reason: str,
    ) -> CmuxActivationIntent:
        """Record that the intent's unbound workspace was closed by exact
        identity after cmux confirmed the close."""

        intent = self._pending_intent(intent_id, action="be reclaimed")
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            self._write_intent_transition(
                connection,
                intent,
                "reclaimed",
                event="reclaimed",
                binding_id=None,
                stamp=stamp,
                extra={
                    "workspace_uuids": list(workspace_uuids),
                    "reason": reason,
                },
            )
        return self.get_intent(intent_id)

    def pending_intents(self) -> tuple[CmuxActivationIntent, ...]:
        rows = self._database.execute(
            "SELECT * FROM cmux_activation_intents "
            "WHERE state = 'pending' ORDER BY created_at ASC, rowid ASC"
        ).fetchall()
        return tuple(_row_to_intent(row) for row in rows)

    def pending_intents_for_cell(
        self, cell_id: str
    ) -> tuple[CmuxActivationIntent, ...]:
        rows = self._database.execute(
            "SELECT * FROM cmux_activation_intents "
            "WHERE state = 'pending' AND cell_id = ? "
            "ORDER BY created_at ASC, rowid ASC",
            (cell_id,),
        ).fetchall()
        return tuple(_row_to_intent(row) for row in rows)

    def get_intent(self, intent_id: str) -> CmuxActivationIntent:
        row = self._database.execute(
            "SELECT * FROM cmux_activation_intents WHERE intent_id = ?",
            (intent_id,),
        ).fetchone()
        if row is None:
            raise KeyError(intent_id)
        return _row_to_intent(row)

    def _pending_intent(self, intent_id: str, *, action: str) -> CmuxActivationIntent:
        intent = self.get_intent(intent_id)
        if intent.state != "pending":
            raise CmuxBindingConflict(
                f"a {intent.state} activation intent cannot {action}"
            )
        return intent

    def _write_intent_transition(
        self,
        connection: Any,
        intent: CmuxActivationIntent,
        state: str,
        *,
        event: str,
        binding_id: str | None,
        stamp: str,
        extra: Mapping[str, Any],
    ) -> None:
        connection.execute(
            "UPDATE cmux_activation_intents "
            "SET state = ?, binding_id = ?, updated_at = ? "
            "WHERE intent_id = ? AND state = 'pending'",
            (state, binding_id, stamp, intent.intent_id),
        )
        payload: dict[str, Any] = {
            "project_key": intent.project_key,
            "cell_id": intent.cell_id,
            "session_id": intent.session_id,
            "profile_alias": intent.profile_alias,
        }
        if binding_id is not None:
            payload["binding_id"] = binding_id
        payload.update(extra)
        self._events.append(
            connection,
            EventInput(
                event_type=f"cmux_intent.{event}",
                aggregate_type="cmux_intent",
                aggregate_id=intent.intent_id,
                payload=payload,
            ),
        )

    def record_classic(self, binding_id: str, session_id: str) -> None:
        """Durably record that this seat runs the classic TUI in-pane.

        The lead-intake transport refuses any surface without this
        evidence, so envelopes can never be typed into a pane that is
        not the classic interactive lead.
        """

        with self._database.transaction() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO cmux_classic_seats("
                "binding_id, session_id, recorded_at) VALUES (?, ?, ?)",
                (binding_id, session_id, self._now().isoformat()),
            )

    def is_classic(self, binding_id: str, session_id: str) -> bool:
        row = self._database.execute(
            "SELECT 1 FROM cmux_classic_seats WHERE binding_id = ? AND session_id = ?",
            (binding_id, session_id),
        ).fetchone()
        return row is not None

    def active(self) -> tuple[CmuxBinding, ...]:
        rows = self._database.execute(
            "SELECT * FROM cmux_surface_bindings WHERE state = 'active' "
            "ORDER BY created_at ASC, rowid ASC"
        ).fetchall()
        return tuple(_row_to_binding(row) for row in rows)

    def residual(self) -> tuple[CmuxBinding, ...]:
        rows = self._database.execute(
            "SELECT * FROM cmux_surface_bindings WHERE state = 'residual' "
            "ORDER BY created_at ASC, rowid ASC"
        ).fetchall()
        return tuple(_row_to_binding(row) for row in rows)

    def residual_for_cell(self, cell_id: str) -> tuple[CmuxBinding, ...]:
        rows = self._database.execute(
            "SELECT * FROM cmux_surface_bindings "
            "WHERE state = 'residual' AND cell_id = ? "
            "ORDER BY created_at ASC, rowid ASC",
            (cell_id,),
        ).fetchall()
        return tuple(_row_to_binding(row) for row in rows)

    def active_orchestrator(self) -> CmuxBinding | None:
        row = self._database.execute(
            "SELECT * FROM cmux_surface_bindings "
            "WHERE role = 'orchestrator' AND state = 'active'"
        ).fetchone()
        return None if row is None else _row_to_binding(row)

    def active_lead(self, cell_id: str) -> CmuxBinding | None:
        row = self._database.execute(
            "SELECT * FROM cmux_surface_bindings "
            "WHERE role = 'lead' AND cell_id = ? AND state = 'active'",
            (cell_id,),
        ).fetchone()
        return None if row is None else _row_to_binding(row)

    def restorable_relaunched_leads(self) -> tuple[tuple[CmuxBinding, str], ...]:
        """Stale lead bindings with no active sibling whose fully-proven
        active anchor still names exactly their session and surface: the
        reboot-relaunched seat a failed recovery retired (INFRA-198)."""

        rows = self._database.execute(
            "SELECT b.*, a.anchor_id AS selected_anchor_id "
            "FROM cmux_surface_bindings b "
            "JOIN channel_trust_anchors a ON a.cell_id = b.cell_id "
            "AND a.state = 'active' AND a.prompt_pattern IS NOT NULL "
            "AND a.session_id = b.session_id "
            "AND a.surface_uuid = b.surface_uuid "
            # Sol finding 2: a stale anchored session and its live
            # process can outlive the cell that named them, so the
            # current cell must still name exactly this seat.
            "JOIN project_cells c ON c.cell_id = b.cell_id "
            "AND c.state = 'active' "
            "AND c.session_id = b.session_id "
            "AND c.profile_alias = b.profile_alias "
            "AND c.lane_role = b.lane_role "
            "WHERE b.role = 'lead' AND b.state = 'stale' "
            "AND NOT EXISTS (SELECT 1 FROM cmux_surface_bindings "
            "WHERE role = 'lead' AND state = 'active' "
            "AND cell_id = b.cell_id)"
        ).fetchall()
        return tuple(
            (_row_to_binding(row), str(row["selected_anchor_id"])) for row in rows
        )

    def restore_lead(
        self,
        binding_id: str,
        *,
        ref: CmuxSurfaceRef,
        reason: str,
        anchor_id: str,
    ) -> CmuxBinding:
        """Flip one stale lead binding back to active at ``ref`` while
        its cell has no active lead; the caller proved the seat alive.
        A reboot re-mints only the WORKSPACE uuid around the surviving
        surface, so workspace drift is absorbed and the same generation
        keeps the running sidecar's capability valid; surface drift
        refuses."""

        current = self.get(binding_id)
        if current.role != "lead" or current.state != "stale":
            raise CmuxBindingConflict(
                f"a {current.state} binding cannot be restored"
            )
        if current.cell_id is None or self.active_lead(str(current.cell_id)):
            raise CmuxBindingConflict("an active binding already owns this cell")
        if ref.surface_uuid != current.surface_uuid:
            raise CmuxBindingConflict("a restored seat must keep its surface")
        cell_id = str(current.cell_id)
        identity = (
            cell_id,
            current.session_id,
            current.profile_alias,
            current.lane_role,
        )
        cell_names_seat = (
            "SELECT 1 FROM project_cells WHERE cell_id = ? "
            "AND state = 'active' AND session_id = ? "
            "AND profile_alias = ? AND lane_role = ?"
        )
        # Sol finding 2: this cell must still name exactly this seat.
        if self._database.execute(cell_names_seat, identity).fetchone() is None:
            raise CmuxBindingConflict("this cell no longer names this seat")
        # Sol finding 1: the reboot re-mints the workspace, so the
        # anchor must be carried onto the workspace this binding is
        # restored into, or the gate compares the live workspace against
        # the anchor's original one and the seat is unregisterable
        # either way. ``rebind`` is that contract, re-measuring every
        # content fact and refusing a pending prompt pattern.
        anchors = ChannelTrustAnchors(self._database, events=self._events)
        anchor = anchors.get(anchor_id)
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            # Both ownership facts re-proved under the write lock: a
            # concurrent rotation or rebinding cannot race restoration
            # into two active owners or an obsolete one.
            if connection.execute(
                cell_names_seat + " AND NOT EXISTS "
                "(SELECT 1 FROM cmux_surface_bindings WHERE role = 'lead' "
                "AND state = 'active' AND cell_id = ?)",
                (*identity, cell_id),
            ).fetchone() is None:
                raise CmuxBindingConflict("this cell no longer names this seat")
            selected_anchor = connection.execute(
                "SELECT 1 FROM channel_trust_anchors WHERE anchor_id = ? "
                "AND state = 'active' AND cell_id = ? AND session_id = ? "
                "AND profile_alias = ? AND surface_uuid = ?",
                (
                    anchor_id,
                    cell_id,
                    current.session_id,
                    current.profile_alias,
                    current.surface_uuid,
                ),
            ).fetchone()
            if selected_anchor is None:
                raise CmuxBindingConflict(
                    "the selected anchor no longer names this seat"
                )
            # The anchor re-mint rides THIS transaction, so the anchor
            # and the binding reach the new workspace together or
            # neither does; a refusal here rolls both back.
            if anchor.workspace_uuid != ref.workspace_uuid:
                entry_path = Path(anchor.canonical_entry_path)
                try:
                    anchors._rebind_in(
                        connection,
                        cell_id=cell_id,
                        profile_alias=anchor.profile_alias,
                        entry_path=entry_path,
                        package_root=entry_path.parents[2],
                        channel_entry=anchor.channel_entry,
                        launch_argv_template=anchor.launch_argv_template,
                        workspace_uuid=ref.workspace_uuid,
                        surface_uuid=anchor.surface_uuid,
                        session_id=anchor.session_id,
                    )
                except Exception as error:
                    raise CmuxBindingConflict(
                        f"the anchor cannot follow the restored seat: {error}"
                    ) from error
            connection.execute(
                "UPDATE cmux_surface_bindings SET workspace_uuid = ? "
                "WHERE binding_id = ?",
                (ref.workspace_uuid, binding_id),
            )
            self._write_transition(
                connection,
                current,
                "active",
                event="bound",
                reason=reason,
                stamp=stamp,
            )
        return self.get(binding_id)

    def active_lead_for_project(
        self, project_key: str, lane_role: str = "development"
    ) -> CmuxBinding | None:
        """The project's active lead binding within one lane.

        INFRA-219 R5 (Sol correction 110ed759): with a harness binding
        now able to coexist with the development one for the SAME
        project, a project-only lookup would be ambiguous — the
        ``ORDER BY created_at DESC LIMIT 1`` used to silently prefer
        whichever bound most recently. ``lane_role`` defaults to
        ``'development'`` so every existing zero-argument caller keeps
        exactly today's single-lane answer.
        """

        row = self._database.execute(
            "SELECT * FROM cmux_surface_bindings "
            "WHERE role = 'lead' AND project_key = ? AND lane_role = ? "
            "AND state = 'active' "
            "ORDER BY created_at DESC LIMIT 1",
            (project_key, lane_role),
        ).fetchone()
        return None if row is None else _row_to_binding(row)

    def get(self, binding_id: str) -> CmuxBinding:
        row = self._database.execute(
            "SELECT * FROM cmux_surface_bindings WHERE binding_id = ?",
            (binding_id,),
        ).fetchone()
        if row is None:
            raise KeyError(binding_id)
        return _row_to_binding(row)

    def next_lead_generation(self, cell_id: str) -> int:
        """The generation the cell's next created binding will carry."""

        return self._next_generation(cell_id=cell_id)

    def _next_generation(
        self, *, role: str = "lead", cell_id: str | None = None
    ) -> int:
        if cell_id is not None:
            highest = self._database.scalar(
                "SELECT max(generation) FROM cmux_surface_bindings WHERE cell_id = ?",
                (cell_id,),
            )
        else:
            highest = self._database.scalar(
                "SELECT max(generation) FROM cmux_surface_bindings WHERE role = ?",
                (role,),
            )
        return int(highest or 0) + 1

    def _insert(
        self,
        *,
        role: str,
        project_key: str | None,
        cell_id: str | None,
        session_id: str | None,
        profile_alias: str | None,
        ref: CmuxSurfaceRef,
        generation: int,
        state: str = "active",
        event: str = "bound",
        reason: str | None = None,
        lane_role: str = "development",
    ) -> CmuxBinding:
        binding_id = self._ids()
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            self._write_insert(
                connection,
                binding_id,
                role=role,
                project_key=project_key,
                cell_id=cell_id,
                session_id=session_id,
                profile_alias=profile_alias,
                ref=ref,
                generation=generation,
                state=state,
                event=event,
                reason=reason,
                stamp=stamp,
                lane_role=lane_role,
            )
        return self.get(binding_id)

    def _write_insert(
        self,
        connection: Any,
        binding_id: str,
        *,
        role: str,
        project_key: str | None,
        cell_id: str | None,
        session_id: str | None,
        profile_alias: str | None,
        ref: CmuxSurfaceRef,
        generation: int,
        state: str,
        event: str,
        reason: str | None,
        stamp: str,
        lane_role: str = "development",
    ) -> None:
        connection.execute(
            "INSERT INTO cmux_surface_bindings("
            "binding_id, role, project_key, cell_id, session_id, "
            "profile_alias, workspace_uuid, surface_uuid, generation, "
            "state, lane_role, created_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                binding_id,
                role,
                project_key,
                cell_id,
                session_id,
                profile_alias,
                ref.workspace_uuid,
                ref.surface_uuid,
                generation,
                state,
                lane_role,
                stamp,
                stamp,
            ),
        )
        payload = _identity_payload(
            role=role,
            project_key=project_key,
            cell_id=cell_id,
            session_id=session_id,
            profile_alias=profile_alias,
            ref=ref,
            generation=generation,
            lane_role=lane_role,
        )
        if reason is not None:
            payload["reason"] = reason
        self._events.append(
            connection,
            EventInput(
                event_type=f"cmux_binding.{event}",
                aggregate_type="cmux_binding",
                aggregate_id=binding_id,
                payload=payload,
            ),
        )

    def _transition(
        self,
        binding: CmuxBinding,
        state: str,
        *,
        event: str,
        reason: str,
    ) -> CmuxBinding:
        if binding.state == state:
            return binding
        if binding.state not in _TRANSITION_SOURCES[state]:
            raise CmuxBindingConflict(
                f"a {binding.state} binding cannot become {state}"
            )
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            self._write_transition(
                connection,
                binding,
                state,
                event=event,
                reason=reason,
                stamp=stamp,
            )
        return self.get(binding.binding_id)

    def _write_transition(
        self,
        connection: Any,
        binding: CmuxBinding,
        state: str,
        *,
        event: str,
        reason: str | None,
        stamp: str,
    ) -> None:
        connection.execute(
            "UPDATE cmux_surface_bindings SET state = ?, updated_at = ? "
            "WHERE binding_id = ? AND state = ?",
            (state, stamp, binding.binding_id, binding.state),
        )
        payload = _identity_payload(
            role=binding.role,
            project_key=binding.project_key,
            cell_id=binding.cell_id,
            session_id=binding.session_id,
            profile_alias=binding.profile_alias,
            ref=binding.ref,
            generation=binding.generation,
            lane_role=binding.lane_role,
        )
        if reason is not None:
            payload["reason"] = reason
        self._events.append(
            connection,
            EventInput(
                event_type=f"cmux_binding.{event}",
                aggregate_type="cmux_binding",
                aggregate_id=binding.binding_id,
                payload=payload,
            ),
        )


@dataclass(frozen=True, slots=True)
class CmuxReconciliationReport:
    """The outcome of one startup pass over the seat bindings.

    ``available`` records whether the socket answered the initial ping;
    ``completed`` whether the whole pass finished without a cmux failure.
    A partial pass (``available`` without ``completed``) left every
    unfinished binding in a recoverable state for the next startup — it
    never aborts the daemon.
    """

    available: bool
    completed: bool = True
    verified: tuple[str, ...] = ()
    replaced: tuple[str, ...] = ()
    lost: tuple[str, ...] = ()
    reclaimed: tuple[str, ...] = ()
    intents_reclaimed: tuple[str, ...] = ()
    intents_aborted: tuple[str, ...] = ()
    intents_ambiguous: tuple[str, ...] = ()



def _lane_cwd(
    project_paths: Mapping[str, Path],
    lane_project_paths: Mapping[tuple[str, str], Path],
    project_key: str,
    lane_role: str,
) -> Path | None:
    """Resolve one lane's checkout, falling back to the project's own.

    INFRA-219 R5b (Sol correction 110ed759): the seater and the
    reconciler resolved a seat's cwd by project alone, so a HARNESS
    binding was restored into the DEVELOPMENT lead's worktree — the
    exact defect Sol reported, and the reason lane identity on
    ``CmuxBinding`` alone was not enough. A lane-keyed override wins
    when present; every lane without one falls back to the historical
    project mapping, so development behavior is byte-compatible with
    today whenever no harness lane is configured.
    """

    override = lane_project_paths.get((project_key, lane_role))
    if override is not None:
        return override
    return project_paths.get(project_key)


class CmuxSurfaceReconciler:
    """Restore visible seats from durable identity before accepting work.

    Each active binding is validated against the exact live workspace and
    surface UUIDs. A missing lead surface is replaced with one new
    generation whose workspace carries the recorded project cwd, the
    profile's CLAUDE_CONFIG_DIR, and the same role-prompted,
    channel-enabled classic launch command normal seating composes (Sol
    correction a06cbce0) — never caller-supplied prompt text, a credential,
    or a blank terminal: when the channel
    launch cannot be built, the seat is recorded lost with one durable
    ``channel.blocked`` receipt instead of an active binding over an
    empty pane. A missing Orchestrator seat is rebound only to this
    process's own cmux seat (from the environment cmux itself injected);
    otherwise it is recorded lost and the operator relaunches the pane,
    so ownership never transfers silently. When the socket denies or
    fails, durable state is left untouched.

    Sol correction c5600e31: a channel-launched recovered lead must
    complete the exact same bounded channel-trust confirmation and
    registration path normal seating runs (:class:`CmuxLeadSeater`)
    BEFORE durable state may call it usable — UNLIKE the seater's
    best-effort, fire-and-forget one-shot trigger, restart recovery
    FAILS CLOSED on the outcome. ``ChannelTrustGate.evaluate``
    (``channel_trust.py``) never produces a confirmed
    :class:`~hermes_orchestrator.channel_trust.TrustVerdict` without a
    freshly re-read, exactly matching development-channel dialog on the
    live surface — there is no persistent-trust bypass anywhere in that
    gate, so a dialog that never appears inside the confirmer's bounded
    watch window is exactly as unproven as one the gate explicitly
    refuses. ``channel_trust.confirm_seat`` returning ``None``, a
    non-confirmed verdict, or raising are therefore all handled
    identically: the successor binding is recorded lost (never left
    active/classic) with one durable ``channel.blocked`` receipt naming
    why, so ``rotate-lead`` can reseat the cell once an operator
    resolves it. A reconciler composed with a launcher but no
    ``channel_trust`` trigger can never obtain that proof either, so it
    fails exactly the same way.

    Sol correction 57c46faa (packet 2): the successor's classic-seat
    evidence (``cmux_classic_seats``) is written only once a confirmed
    verdict actually comes back — never up front — and a failed or
    unproven confirmation closes the exact newly created workspace
    through the same compensated-closure lifecycle used elsewhere in
    this module before the binding is marked lost, so a failed or
    unproven trust decision can never leave stale classic evidence or
    an unowned live cmux surface behind. When cmux cannot confirm that
    close, the binding is held as residual ownership evidence instead
    of being marked lost outright, so the exact surface is reclaimed by
    a later reconciliation pass rather than leaked.
    """

    def __init__(
        self,
        *,
        bindings: CmuxSurfaceBindings,
        port: CmuxControlPort,
        project_paths: Mapping[str, Path],
        lane_project_paths: Mapping[tuple[str, str], Path] | None = None,
        prompt_files: Mapping[str, Path] | None = None,
        profile_dirs: ProfileDirectory,
        environ: Mapping[str, str],
        channel_launch: ChannelLaunchSource | None = None,
        control: ControlOperations | None = None,
        channel_trust: ChannelTrustTrigger | None = None,
        worker_alive: Callable[[str], bool | None] | None = None,
    ) -> None:
        # Seam only: the default IS the real measurement, so no
        # composition site needs wiring and tests need no live ``ps``.
        self._worker_alive = (
            worker_alive if worker_alive is not None else managed_claude_worker_alive
        )
        self._bindings = bindings
        self._port = port
        self._project_paths = dict(project_paths)
        # INFRA-219 R5b: lane-keyed checkout overrides, so a harness
        # seat/binding never resolves into the development worktree.
        self._lane_project_paths = dict(lane_project_paths or {})
        self._prompt_files = dict(prompt_files or {})
        self._profile_dirs = profile_dirs
        self._environ = dict(environ)
        self._channel_launch = channel_launch
        self._control = control
        self._channel_trust = channel_trust

    def own_seat(self) -> CmuxSurfaceRef | None:
        """The cmux seat this very process runs in, if any."""

        workspace = self._environ.get(CMUX_WORKSPACE_ID_ENV, "").strip()
        surface = self._environ.get(CMUX_SURFACE_ID_ENV, "").strip()
        if not workspace or not surface:
            return None
        return CmuxSurfaceRef(workspace_uuid=workspace, surface_uuid=surface)

    async def reconcile(self) -> CmuxReconciliationReport:
        try:
            await self._port.ping()
        except CmuxError:
            # Denial or unavailability fails closed: durable bindings are
            # the truth and stay untouched until cmux is reachable.
            return CmuxReconciliationReport(available=False, completed=False)
        verified: list[str] = []
        replaced: list[str] = []
        lost: list[str] = []
        reclaimed: list[str] = []
        intents_reclaimed: list[str] = []
        intents_aborted: list[str] = []
        intents_ambiguous: list[str] = []
        completed = True
        try:
            # Interrupted activations resolve first: each pending
            # write-ahead intent either reclaims the exact workspace its
            # unique identity created, is aborted, or is surfaced as
            # ambiguous, before any binding is verified or replaced.
            await _resolve_pending_intents(
                self._port,
                self._bindings,
                self._bindings.pending_intents(),
                reclaimed=intents_reclaimed,
                aborted=intents_aborted,
                ambiguous=intents_ambiguous,
            )
            await self._reclaim_residuals(reclaimed, lost)
            for binding in self._bindings.active():
                if await self._port.surface_alive(binding.ref):
                    verified.append(binding.binding_id)
                    continue
                if binding.role == "orchestrator":
                    self._reconcile_orchestrator(binding, replaced, lost)
                else:
                    await self._reconcile_lead(binding, replaced, lost)
            seat = self.own_seat()
            if seat is not None and self._bindings.active_orchestrator() is None:
                self._bindings.bind_orchestrator(seat)
            await self._restore_relaunched_trusted_leads(replaced)
        except CmuxError:
            # cmux failed mid-pass. Visibility is optional: every
            # unfinished binding keeps its recoverable durable state, no
            # further cmux mutation is attempted, and the partial report
            # lets daemon startup continue; the next pass retries.
            completed = False
        return CmuxReconciliationReport(
            available=True,
            # An ambiguous intent is unresolved ownership evidence: the
            # pass is honestly incomplete until an operator resolves it.
            completed=completed and not intents_ambiguous,
            verified=tuple(verified),
            replaced=tuple(replaced),
            lost=tuple(lost),
            reclaimed=tuple(reclaimed),
            intents_reclaimed=tuple(intents_reclaimed),
            intents_aborted=tuple(intents_aborted),
            intents_ambiguous=tuple(intents_ambiguous),
        )

    async def _reclaim_residuals(self, reclaimed: list[str], lost: list[str]) -> None:
        """Resolve workspaces whose earlier close was never confirmed.

        Each residual row is ownership evidence for an exact workspace
        UUID. One that is no longer live is recorded lost; a live one is
        closed and journaled. A cmux failure leaves the row residual —
        still owned, still blocking hibernation — for the next startup.
        """

        await _resolve_residual_bindings(
            self._port,
            self._bindings,
            self._bindings.residual(),
            reclaimed=reclaimed,
            lost=lost,
        )

    def _reconcile_orchestrator(
        self,
        binding: CmuxBinding,
        replaced: list[str],
        lost: list[str],
    ) -> None:
        seat = self.own_seat()
        if seat is not None:
            successor = self._bindings.replace(
                binding.binding_id, seat, reason="orchestrator_reseated"
            )
            replaced.append(successor.binding_id)
        else:
            self._bindings.mark_lost(
                binding.binding_id, reason="orchestrator_surface_missing"
            )
            lost.append(binding.binding_id)

    async def _restore_relaunched_trusted_leads(self, replaced: list[str]) -> None:
        """Restore a reboot-relaunched, still-trusted seat that a failed
        recovery attempt left behind (INFRA-198, observed 2026-09-01):
        the relaunched pane, its managed process, its capability config,
        and its stale binding all still exist — only the successor
        attempt's early trust measurement raced and failed — so when the
        exact seat the proven anchor names is demonstrably alive, its
        own binding flips back to active and the already-running sidecar
        registers with the generation it already carries. No relaunch,
        no new capability, no keypress; a seat not provably alive stays
        exactly as it is."""

        for binding, anchor_id in self._bindings.restorable_relaunched_leads():
            if binding.session_id is None:
                continue
            try:
                alive = self._worker_alive(str(binding.session_id))
            except Exception:
                alive = None
            if alive is not True:
                continue
            # The surviving surface is located in whatever live
            # workspace holds it today — port reads only, never a create.
            ref = None
            for workspace_uuid in sorted(await self._port.live_workspace_uuids()):
                candidate = CmuxSurfaceRef(
                    workspace_uuid=workspace_uuid,
                    surface_uuid=binding.ref.surface_uuid,
                )
                if await self._port.surface_alive(candidate):
                    ref = candidate
                    break
            if ref is None:
                continue
            try:
                restored = self._bindings.restore_lead(
                    binding.binding_id,
                    ref=ref,
                    reason="relaunched_seat_restored",
                    anchor_id=anchor_id,
                )
            except CmuxBindingConflict:
                continue
            replaced.append(restored.binding_id)

    async def _reconcile_lead(
        self,
        binding: CmuxBinding,
        replaced: list[str],
        lost: list[str],
    ) -> None:
        assert binding.project_key is not None
        assert binding.profile_alias is not None
        cwd = _lane_cwd(
            self._project_paths,
            self._lane_project_paths,
            binding.project_key,
            getattr(binding, "lane_role", "development"),
        )
        try:
            config_dir = self._profile_dirs.config_dir(binding.profile_alias)
        except (KeyError, ValueError):
            config_dir = None
        if cwd is None or config_dir is None:
            # The recorded identity can no longer be satisfied exactly;
            # adopting a different project path or profile would break
            # isolation, so the seat is recorded lost instead.
            self._bindings.mark_lost(
                binding.binding_id,
                reason=("project_missing" if cwd is None else "profile_missing"),
            )
            lost.append(binding.binding_id)
            return
        cell_id = str(binding.cell_id)
        session_id = str(binding.session_id)
        # Sol correction a06cbce0: restart recovery relaunches the same
        # managed channel-enabled classic seat normal seating composes.
        # The session-scoped config is stamped with the generation the
        # successor binding will carry — the predecessor is still the
        # cell's highest generation, so ``next_lead_generation`` reports
        # exactly the successor's — and only the sanitized extension
        # grammar can carry it, so the hub's generation check passes for
        # the reseated lead and for nothing else.
        try:
            if self._channel_launch is None:
                raise CmuxBindingConflict(
                    "no channel launcher is composed for restart recovery"
                )
            config = self._channel_launch.generate(
                project_key=binding.project_key,
                cell_id=cell_id,
                session_id=session_id,
                profile_alias=binding.profile_alias,
                generation=self._bindings.next_lead_generation(cell_id),
            )
            command = classic_channel_command(
                session_id,
                resume=True,
                channel_config=config,
                prompt_file=self._prompt_files.get(
                    getattr(binding, "lane_role", "development")
                ),
            )
        except Exception as error:
            # Fail closed: no launch command, no replacement seat. An
            # active binding over a blank terminal would satisfy the
            # ledger while running no Claude process and registering no
            # channel, so the seat is recorded lost — never silently:
            # one durable, actionable channel.blocked receipt records
            # exactly why, and rotate-lead reseats the cell once the
            # sidecar build or launcher composition is repaired.
            if self._control is not None:
                with suppress(Exception):
                    self._control.record(
                        kind="channel.blocked",
                        project_key=binding.project_key,
                        cell_id=cell_id,
                        session_id=session_id,
                        result={"launcher_error": str(error)[:200]},
                        reason=(
                            "the channel-enabled classic launch command "
                            "could not be built during restart "
                            "recovery; the seat is recorded lost "
                            "instead of an active binding over a blank "
                            "terminal — repair the sidecar build or "
                            "launcher composition, then rotate-lead "
                            "reseats the cell"
                        ),
                    )
            self._bindings.mark_lost(
                binding.binding_id, reason="channel_launch_unavailable"
            )
            lost.append(binding.binding_id)
            return
        successor = await _activate_lead_seat(
            self._port,
            self._bindings,
            project_key=binding.project_key,
            cwd=cwd,
            config_dir=config_dir,
            cell_id=cell_id,
            session_id=session_id,
            profile_alias=binding.profile_alias,
            replacing=binding.binding_id,
            replace_reason="surface_missing",
            command=command,
        )
        # Sol correction 57c46faa (packet 2): classic evidence is
        # deferred until trust is actually confirmed. Recording it
        # up front (as normal seating does) let a failed or unproven
        # confirmation leave a stale ``cmux_classic_seats`` row behind
        # for a seat whose active ownership was about to be
        # relinquished. Deferring cannot deadlock the sidecar's
        # registration: the development channel — and therefore the
        # sidecar that registers over it — only actually loads once
        # this exact gate presses Enter on the dialog, which is the
        # same synchronous call whose confirmed return is required
        # below before classic evidence is written; no registration
        # can arrive first.
        #
        # Sol correction c5600e31: restart recovery fails closed on
        # trust — see the class docstring for the exact contract.
        # ``confirm_seat`` runs the same bounded watch-then-gate path
        # normal seating triggers, for this exact successor binding;
        # only an explicit confirmed verdict retains the active/classic
        # seat.
        verdict: TrustVerdict | None = None
        trust_error: str | None = None
        try:
            if self._channel_trust is None:
                raise CmuxBindingConflict(
                    "no channel-trust trigger is composed for restart "
                    "recovery"
                )
            verdict = await self._channel_trust.confirm_seat(successor)
        except Exception as error:
            trust_error = str(error)[:200]
        if verdict is not None and verdict.confirmed:
            # The classic-seat evidence the lead-intake transport and
            # the channel hub both require, recorded exactly as normal
            # seating records it — but only now that trust is actually
            # proven, never before.
            self._bindings.record_classic(successor.binding_id, session_id)
            replaced.append(successor.binding_id)
            return
        # Unconfirmed, refused, ambiguous, or never-attempted: durable
        # state may not call this seat usable, and no classic evidence
        # was ever written for it. The exact newly created workspace is
        # closed through the same compensated-closure lifecycle used
        # elsewhere in this module (``_release_pending_seat``) so a
        # failed or unproven trust decision never leaves an unowned
        # live cmux surface behind: only once cmux confirms the close
        # is the binding marked lost. When the close itself cannot be
        # confirmed, the binding is held as residual ownership evidence
        # instead — never active, never classic — so a later
        # reconciliation pass (``_reclaim_residuals``) reclaims the
        # exact surface rather than leaking it.
        try:
            await self._port.close_workspace(successor.workspace_uuid)
        except CmuxError:
            self._bindings.mark_residual(
                successor.binding_id,
                reason="channel_trust_unconfirmed_close_uncertain",
            )
        else:
            self._bindings.mark_lost(
                successor.binding_id, reason="channel_trust_unconfirmed"
            )
            lost.append(successor.binding_id)
        # The gate itself already records its own receipt for a refusal
        # it actually reached (e.g. ``channel.approval_required``); this
        # receipt is the seat-level record that restart recovery could
        # not hand off a usable lead, covering the timeout/no-trigger/
        # closure-ambiguous cases the gate never sees too.
        if self._control is not None:
            with suppress(Exception):
                self._control.record(
                    kind="channel.blocked",
                    project_key=binding.project_key,
                    cell_id=cell_id,
                    session_id=session_id,
                    result={
                        "successor_binding_id": successor.binding_id,
                        "first_failure": (
                            verdict.first_failure
                            if verdict is not None
                            else None
                        ),
                        "trigger_error": trust_error,
                    },
                    reason=(
                        "CHANNEL TRUST UNCONFIRMED: the restart-recovered "
                        "lead seat did not complete the bounded channel-"
                        "trust confirmation and registration path within "
                        "the window; the seat is recorded lost instead "
                        "of an active binding over an unconfirmed "
                        "channel — confirm the development-channel "
                        "dialog manually, then rotate-lead reseats the "
                        "cell"
                    ),
                )


async def _activate_lead_seat(
    port: CmuxControlPort,
    bindings: CmuxSurfaceBindings,
    *,
    project_key: str,
    cwd: Path,
    config_dir: Path,
    cell_id: str,
    session_id: str,
    profile_alias: str,
    lane_role: str = "development",
    replacing: str | None = None,
    replace_reason: str | None = None,
    command: str | None = None,
) -> CmuxBinding:
    """Create, prepare, and durably bind one lead seat as a write-ahead
    compensated activation from durable identity alone.

    The activation intent — a durable unique operation identity — commits
    before the external create, and the create carries that identity
    inside the workspace title. The workspace carries the recorded
    project cwd and the profile's exact CLAUDE_CONFIG_DIR; its native
    resume command is the ``--resume`` form of the exact, already
    grammar-validated ``command`` that was launched (so a restored pane
    keeps its channel extension), or the sanitized plain
    ``claude --resume <session>`` when none was given — never a prompt
    or credential. Once cmux returns, the identities bind
    atomically to the intent as a residual row, then promote to the
    active seat (retiring ``replacing`` in the same transaction). A
    failure or crash at any point therefore leaves either nothing, a
    pending intent whose marker finds the exact workspace, or a
    recoverable residual — never an untracked live workspace.
    """

    title = f"{project_key} lead"
    # INFRA-214 (observed live 2026-09-01): the lane never reached the
    # activation intent, so BOTH failed harness seats persisted as
    # ``lane_role=development`` and their residue could not be told
    # apart from the development lane's own binding. ``record_intent``
    # has always accepted the lane; the seat simply never passed it.
    intent = bindings.record_intent(
        project_key=project_key,
        cell_id=cell_id,
        session_id=session_id,
        profile_alias=profile_alias,
        lane_role=lane_role,
    )
    # The marker resolves cmux 0.64.22's short mutation acknowledgement
    # (``OK workspace:<n>``) to exactly one workspace through the
    # metadata listing; zero or multiple matches fail closed inside the
    # adapter.
    env: dict[str, str] = {"CLAUDE_CONFIG_DIR": str(config_dir)}
    ref = await port.create_workspace(
        title=f"{title} {intent.title_marker}",
        cwd=cwd,
        command=command,
        env=env,
        resolve_marker=intent.title_marker,
    )
    try:
        pending = bindings.bind_intent(intent.intent_id, ref=ref)
    except Exception:
        # The returned identities could not be durably bound; closing the
        # workspace keeps every live seat inside durable ownership, and
        # an unconfirmed close leaves the pending intent to find it again.
        with suppress(CmuxError):
            await port.close_workspace(ref.workspace_uuid)
        raise
    try:
        await port.set_surface_resume(
            ref,
            # A restore must always RESUME the session: the launched
            # command's ``--session-id`` form (fresh session) maps to
            # ``--resume`` while keeping the exact channel extension.
            classic_resume_command(session_id, resume=True)
            if command is None
            else command.replace(
                f"--session-id {session_id}", f"--resume {session_id}", 1
            ),
        )
        active = bindings.activate_residual(
            pending.binding_id, replacing=replacing, reason=replace_reason
        )
    except Exception:
        await _release_pending_seat(port, bindings, pending)
        raise
    with suppress(CmuxError):
        # The marker's correlation job ended when the identities bound;
        # the visible title is cosmetic.
        await port.rename_workspace(ref.workspace_uuid, title)
    return active


async def _release_pending_seat(
    port: CmuxControlPort,
    bindings: CmuxSurfaceBindings,
    pending: CmuxBinding,
) -> None:
    """Compensate a write-ahead seat whose activation did not finish.

    The exact workspace is closed by UUID and its row retired; if cmux
    cannot confirm the close, the row simply stays residual — recoverable
    ownership that blocks hibernation until reconciliation resolves it.
    """

    try:
        await port.close_workspace(pending.workspace_uuid)
    except CmuxError:
        return
    bindings.mark_closed(pending.binding_id, reason="activation_abandoned")


async def _resolve_pending_intents(
    port: CmuxControlPort,
    bindings: CmuxSurfaceBindings,
    intents: tuple[CmuxActivationIntent, ...],
    *,
    reclaimed: list[str] | None = None,
    aborted: list[str] | None = None,
    ambiguous: list[str] | None = None,
) -> None:
    """Resolve write-ahead activations that never bound their identities.

    Correlation is only through each intent's unique title marker, and a
    workspace is reclaimed only when exactly one live workspace carries
    it: the intent's single create can own one workspace, so a second
    exact match (operator duplication, copied metadata) means ownership
    cannot be uniquely proven and nothing is closed — the intent stays
    pending, blocking hibernation, until an operator resolves it. With
    ``ambiguous`` given, the ambiguity is recorded there and resolution
    continues; without it, the ambiguity raises
    :class:`CmuxBindingConflict` and refuses the caller's activation. No
    match means the create never happened (or its workspace already
    vanished) and the intent is aborted. Unrelated workspaces are never
    adopted or closed, and a cmux failure propagates with the intent
    still pending — still owned, still blocking hibernation — for the
    next pass.
    """

    for intent in intents:
        matches = await port.find_workspace_uuids(title_marker=intent.title_marker)
        if not matches:
            bindings.abort_intent(
                intent.intent_id, reason="no_workspace_carries_identity"
            )
            if aborted is not None:
                aborted.append(intent.intent_id)
            continue
        if len(matches) > 1:
            if ambiguous is None:
                raise CmuxBindingConflict(
                    "multiple workspaces carry one activation intent's "
                    "identity; ownership requires operator resolution"
                )
            ambiguous.append(intent.intent_id)
            continue
        [workspace_uuid] = matches
        await port.close_workspace(workspace_uuid)
        bindings.reclaim_intent(
            intent.intent_id,
            workspace_uuids=(workspace_uuid,),
            reason="unbound_workspace_reclaimed",
        )
        if reclaimed is not None:
            reclaimed.append(intent.intent_id)


async def _resolve_residual_bindings(
    port: CmuxControlPort,
    bindings: CmuxSurfaceBindings,
    residuals: tuple[CmuxBinding, ...],
    *,
    reclaimed: list[str] | None = None,
    lost: list[str] | None = None,
) -> None:
    """Resolve workspaces whose earlier lifecycle was never confirmed.

    Each residual row is ownership evidence for an exact workspace UUID.
    One that is no longer live is recorded lost; a live one is closed and
    journaled. A cmux failure propagates with the row still residual —
    still owned, still blocking hibernation — for the caller's boundary.
    """

    if not residuals:
        return
    live = await port.live_workspace_uuids()
    for binding in residuals:
        if binding.workspace_uuid not in live:
            bindings.mark_lost(binding.binding_id, reason="residual_workspace_vanished")
            if lost is not None:
                lost.append(binding.binding_id)
            continue
        await port.close_workspace(binding.workspace_uuid)
        bindings.mark_closed(binding.binding_id, reason="residual_reclaimed")
        if reclaimed is not None:
            reclaimed.append(binding.binding_id)


#: The project-cell state a dead lead's cell is retired FROM, and the
#: state it is retired INTO. 'failed' is the vocabulary's existing
#: non-occupying terminal (``ProjectCellService._fail_unconfirmed_start``
#: writes exactly it), so retiring into it releases the
#: one-active-cell-per-lane index and a deliberate ``start-lane`` can
#: seat the replacement.
_DEAD_LEAD_CELL_STATE = "active"
_DEAD_LEAD_RETIRED_CELL_STATE = "failed"
#: The binding-closure reason a dead worker's seat carries, so the
#: journal distinguishes this retirement from a rotation or a close.
_DEAD_LEAD_BINDING_REASON = "dead_worker_surface_missing"
#: Which of the two independent probes condemned a seat, recorded on
#: the retirement receipt so an operator reading the journal can tell
#: "the workspace was closed" from "the workspace is still open but
#: the worker inside it exited".
_DEAD_LEAD_SIGNAL_SURFACE = "surface_absent"
_DEAD_LEAD_SIGNAL_WORKER = "worker_absent"


class CmuxDeadLeadSweep:
    """Retire lead seats whose surface OR whose worker is provably gone.

    INFRA-198: :meth:`CmuxSurfaceReconciler.reconcile` runs exactly once,
    before the supervisor starts, so a lead whose workspace was closed
    (or whose Claude process exited) MID-RUN stayed durably active until
    the next daemon restart — observed live as a cell and binding still
    reading ``active`` on a dead workspace more than two ticks later,
    blocking the replacement launch. This is the per-tick sweep that
    closes that gap, and it retires rather than relaunches: startup
    recovery deliberately keeps replacing a missing seat, while the
    mid-run case hands the cell back so ``start-lane`` seats the
    replacement.

    Every step fails closed, in the same order and for the same reason
    the reconciler's pass does:

    * ``ping()`` first. On :class:`CmuxError` NOTHING is retired — an
      unreachable or denying socket must never read as "every worker
      died", which would tear down healthy seats on a transient hiccup.
    * Absence must come from a SUCCESSFUL ``surface_alive`` probe, never
      from an exception, and a live surface is left completely untouched.
    * A provably absent surface retires that exact identity and only it:
      its binding is closed, its cell leaves ``active`` under a
      compare-and-swap on the exact cell id AND the state just observed
      (so a concurrent transition is never clobbered), any cleanup claim
      held on that cell's issue worktree lease is released, and one
      durable control receipt names the retired cell, binding and
      workspace.
    * A :class:`CmuxError` mid-pass ends the sweep with the remaining
      bindings untouched; the next tick re-derives everything.

    INFRA-198 (observed live): the surface probe alone is not enough.
    Workspace C39EBCDC / surface F5B1BD55 still EXISTED while the
    worker inside it — ``claude --resume 9b539c86`` — had exited with
    "No conversation found", leaving a bare shell prompt in the pane.
    ``surface_alive`` answered True forever, so the seat was never
    retired and ``start-lane`` kept reporting ``already_running`` for a
    dead worker. A seat is now dead when EITHER probe is definitive
    about absence: the surface is provably gone, OR
    :func:`managed_claude_worker_alive` returns exactly ``False``. Its
    ``None`` (``ps`` failed, or ambiguous matches) is an UNKNOWN and
    never retires anything — the same fail-closed rule the surface
    probe already follows.
    """

    def __init__(
        self,
        *,
        bindings: CmuxSurfaceBindings,
        port: CmuxControlPort,
        database: Database,
        events: EventStore,
        control: ControlOperations | None = None,
        leases: WorktreeLeases | None = None,
        profiles: Any | None = None,
        now: Callable[[], datetime] | None = None,
        worker_alive: Callable[[str], bool | None] | None = None,
    ) -> None:
        self._bindings = bindings
        self._port = port
        self._database = database
        self._events = events
        self._control = control
        self._leases = leases
        self._profiles = profiles
        self._now = now or (lambda: datetime.now(UTC))
        # Seam only: the default IS the real measurement, so production
        # composition needs no wiring and tests need no live ``ps``.
        self._worker_alive = (
            worker_alive if worker_alive is not None else managed_claude_worker_alive
        )

    async def tick(self) -> tuple[str, ...]:
        """Retire every active lead binding whose seat is provably dead."""

        try:
            await self._port.ping()
        except CmuxError:
            # Denial or unavailability fails closed: durable bindings are
            # the truth and stay untouched until cmux is reachable.
            return ()
        retired: list[str] = []
        try:
            for binding in self._bindings.active():
                if binding.role != "lead":
                    continue
                signal = await self._condemning_signal(binding)
                if signal is None:
                    continue
                self._retire(binding, signal=signal)
                retired.append(binding.binding_id)
        except CmuxError:
            # cmux failed mid-pass. Absence is only ever a successful
            # probe's answer, so every unprobed binding keeps its durable
            # state and the next tick retries.
            return tuple(retired)
        return tuple(retired)

    async def _condemning_signal(self, binding: CmuxBinding) -> str | None:
        """Which probe proves this seat dead, or ``None`` for "alive".

        Two independent probes, each of which must be DEFINITIVE about
        absence before it condemns anything. The surface probe runs
        first and short-circuits, so a seat whose workspace is already
        gone never pays for a ``ps``.
        """

        if not await self._port.surface_alive(binding.ref):
            return _DEAD_LEAD_SIGNAL_SURFACE
        try:
            alive = self._worker_alive(str(binding.session_id))
        except Exception:
            # A probe that could not run is not evidence of a dead
            # worker; the seat keeps its durable state for the next tick.
            alive = None
        if alive is False:
            return _DEAD_LEAD_SIGNAL_WORKER
        # True (alive) and None (unknown, therefore treated as alive)
        # both leave the seat exactly as found.
        return None

    def _retire(self, binding: CmuxBinding, *, signal: str) -> None:
        """Retire one exact dead identity, in the fail-closed order."""

        project_key = str(binding.project_key)
        cell_id = str(binding.cell_id)
        session_id = str(binding.session_id)
        self._bindings.mark_closed(
            binding.binding_id, reason=_DEAD_LEAD_BINDING_REASON
        )
        cell_retired = self._retire_cell(cell_id, binding)
        lease_id = self._release_issue_lease(project_key, cell_id, session_id)
        if self._control is None:
            return
        with suppress(Exception):
            self._control.record(
                kind="lead.dead_worker_retired",
                project_key=project_key,
                cell_id=cell_id,
                session_id=session_id,
                result={
                    "cell_id": cell_id,
                    "binding_id": binding.binding_id,
                    "workspace_uuid": binding.workspace_uuid,
                    "surface_uuid": binding.surface_uuid,
                    "lane_role": binding.lane_role,
                    "cell_retired": cell_retired,
                    "worktree_lease_released": lease_id,
                    "condemning_signal": signal,
                },
                reason=(
                    (
                        "the lead's cmux surface is provably gone"
                        if signal == _DEAD_LEAD_SIGNAL_SURFACE
                        else "the lead's cmux surface still exists but its "
                        "managed claude worker is provably gone"
                    )
                    + "; its binding is closed and the cell released so a "
                    "deliberate start-lane can seat the replacement"
                ),
            )

    def _retire_cell(self, cell_id: str, binding: CmuxBinding) -> bool:
        """Move the dead lead's cell out of 'active', CAS'd exactly.

        The state is read first and then required to be unchanged by the
        UPDATE, so a rotation, handoff or failure that landed between the
        probe and this write wins instead of being clobbered.
        """

        row = self._database.execute(
            "SELECT state, project_key, profile_alias, lane_role "
            "FROM project_cells WHERE cell_id = ?",
            (cell_id,),
        ).fetchone()
        if row is None:
            return False
        observed = str(row["state"])
        if observed != _DEAD_LEAD_CELL_STATE:
            # Only an occupied cell is retired; anything else is already
            # someone else's transition and stays exactly as found.
            return False
        cell_project_key = str(row["project_key"])
        cell_profile_alias = str(row["profile_alias"] or "")
        cell_lane_role = str(row["lane_role"] or "development")
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE project_cells SET state = ?, updated_at = ? "
                "WHERE cell_id = ? AND state = ?",
                (_DEAD_LEAD_RETIRED_CELL_STATE, stamp, cell_id, observed),
            )
            if cursor.rowcount != 1:
                return False
            # Sol correction cde70842: retiring the cell alone is not
            # enough. The profile lease outlives it, so the replacement
            # _create_cell() reuses the same (project, lane) affinity,
            # its own lease insert then conflicts, and with no active
            # cell left to recover, start-lane FAILS instead of seating
            # the replacement -- defeating the entire point of the
            # sweep. The exact project/lane lease is released here, in
            # the same transaction that retires the cell, so the two can
            # never disagree.
            connection.execute(
                "DELETE FROM profile_leases WHERE project_key = ? "
                "AND profile_alias = ? AND lane_role = ?",
                (cell_project_key, cell_profile_alias, cell_lane_role),
            )
            self._events.append(
                connection,
                EventInput(
                    event_type="project_cell.dead_worker_retired",
                    aggregate_type="project_cell",
                    aggregate_id=cell_id,
                    payload={
                        "project_key": binding.project_key,
                        "session_id": binding.session_id,
                        "lane_role": binding.lane_role,
                        "binding_id": binding.binding_id,
                        "workspace_uuid": binding.workspace_uuid,
                        "previous_state": observed,
                        "state": _DEAD_LEAD_RETIRED_CELL_STATE,
                    },
                ),
            )
        # The in-memory pool must agree with the durable row it just
        # lost, or the next _create_cell() hands back the retired
        # affinity from cache and conflicts all over again.
        if self._profiles is not None and cell_profile_alias:
            with suppress(Exception):
                self._profiles.release(
                    cell_project_key,
                    _DEAD_LEAD_BINDING_REASON,
                    lane_role=cell_lane_role,
                )
        return True

    def retire_dead_lead_cell(self, cell_id: str) -> bool:
        """Retire one cell's dead lead seat for a caller outside a tick.

        INFRA-219 (observed live): ``start-lane`` measures a dead
        worker directly for a cell whose lead binding is no longer
        ``active`` -- e.g. ``stale`` after seat-restore correctly
        refused a dead worker -- so the cell is invisible to
        :meth:`tick`, which only iterates ACTIVE lead bindings. This
        exposes the exact same retirement for that cell-scoped caller
        instead of leaving the lane stuck behind a manual teardown.

        The cell's most recent lead binding in ANY state is looked up
        as the identity evidence for the receipt (there may be no
        active one); it is closed via
        :meth:`CmuxSurfaceBindings.mark_closed` ONLY if it is currently
        active. Retirement itself is the exact ``_retire_cell`` and
        ``_release_issue_lease`` bodies :meth:`tick` uses -- zero
        duplicated transaction logic -- and never touches
        ``lead_assignments``. Returns False (and changes nothing) when
        the cell is not currently 'active': ``_retire_cell`` re-checks
        the cell's state under its own CAS before any write, so a cell
        that has already moved on is left exactly as found.
        """

        row = self._database.execute(
            "SELECT * FROM cmux_surface_bindings WHERE role = 'lead' "
            "AND cell_id = ? ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (cell_id,),
        ).fetchone()
        if row is None:
            return False
        binding = _row_to_binding(row)
        cell_retired = self._retire_cell(cell_id, binding)
        if not cell_retired:
            return False
        if binding.state == "active":
            binding = self._bindings.mark_closed(
                binding.binding_id, reason=_DEAD_LEAD_BINDING_REASON
            )
        lease_id = self._release_issue_lease(
            str(binding.project_key), cell_id, str(binding.session_id)
        )
        if self._control is not None:
            with suppress(Exception):
                self._control.record(
                    kind="lead.dead_worker_retired",
                    project_key=str(binding.project_key),
                    cell_id=cell_id,
                    session_id=str(binding.session_id),
                    result={
                        "cell_id": cell_id,
                        "binding_id": binding.binding_id,
                        "workspace_uuid": binding.workspace_uuid,
                        "surface_uuid": binding.surface_uuid,
                        "lane_role": binding.lane_role,
                        "cell_retired": cell_retired,
                        "worktree_lease_released": lease_id,
                        "condemning_signal": _DEAD_LEAD_SIGNAL_WORKER,
                    },
                    reason=(
                        "start-lane measured the lead's managed claude "
                        "worker provably gone for a cell whose lead "
                        "binding was not active; its binding is closed "
                        "and the cell released so start-lane can seat "
                        "the replacement in the same invocation"
                    ),
                )
        return True

    def _release_issue_lease(
        self, project_key: str, cell_id: str, session_id: str
    ) -> str | None:
        """Release the cleanup claim held on this cell's issue lane.

        The lane lease itself is NOT reclaimed: reclamation requires a
        checkpoint plus a proven remote (:class:`WorktreeCustodian`), and
        a dead worker is exactly the case whose work may be unpushed, so
        inventing a release here would destroy it. What a dead worker CAN
        leave behind is a cleanup claim — while a lease is 'reclaiming'
        the registry refuses every new attachment to its path, so the
        replacement lead could not use the lane at all. That claim is
        released back to 'checkpointed' through the same
        ``release_cleanup`` transition the custodian's own failure paths
        use, under the owner token durably recorded on the row.
        """

        if self._leases is None:
            return None
        row = self._database.execute(
            "SELECT issue_id FROM lead_assignments "
            "WHERE cell_id = ? AND session_id = ? AND state != 'superseded' "
            "ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (cell_id, session_id),
        ).fetchone()
        if row is None:
            return None
        issue_id = str(row["issue_id"])
        for lease in self._leases.active(project_key):
            if lease.issue_id != issue_id:
                continue
            owner = lease.cleanup_owner
            if lease.state != "reclaiming" or not owner:
                return None
            try:
                self._leases.release_cleanup(
                    lease.lease_id,
                    owner=owner,
                    reason=_DEAD_LEAD_BINDING_REASON,
                )
            except CleanupBlocked:
                # The claim moved under us; whoever holds it now owns
                # the outcome and nothing here overrides them.
                return None
            return lease.lease_id
        return None


def _managed_claude_process_lines(session_id: str) -> list[str] | None:
    """The ``ps`` lines of managed ``claude`` processes for one session.

    The single measurement and the single matching predicate both
    :func:`live_claude_argv` and :func:`managed_claude_worker_alive`
    read, so the argv the trust gate compares and the liveness the
    dead-lead sweep decides on can never disagree about what "the
    managed worker for this session" means. ``None`` means the
    measurement itself failed (``ps`` unavailable, denied, or timed
    out) and is therefore no evidence at all — distinct from an empty
    list, which is a successful ``ps`` that found nothing.
    """

    try:
        listing = subprocess.run(
            ["ps", "-axo", "args="],
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    return [
        line.strip()
        for line in listing.splitlines()
        if session_id in line
        and "claude" in line
        and (" --resume " in line or " --session-id " in line)
        and "ps -axo" not in line
    ]


def live_claude_argv(session_id: str) -> list[str]:
    """The live ``claude`` process argv for one exact session.

    Read as an argv vector at trust-gate time so a settings JSON token
    containing spaces stays one token. Zero or two matching processes
    both return an empty argv — the gate then fails closed rather than
    guessing.
    """

    matches: list[list[str]] = []
    for process in psutil.process_iter(["cmdline"]):
        try:
            argv = [str(token) for token in (process.info["cmdline"] or [])]
        except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess):
            continue
        if (
            session_id in argv
            and any(flag in argv for flag in ("--resume", "--session-id"))
            and any("claude" in Path(token).name.lower() for token in argv[:2])
        ):
            matches.append(argv)
    if len(matches) != 1:
        return []
    return matches[0]


def managed_claude_worker_alive(session_id: str) -> bool | None:
    """Tri-state: is this session's managed ``claude`` root process up?

    INFRA-198 (observed live): cmux workspace C39EBCDC / surface
    F5B1BD55 still EXISTED while ``claude --resume 9b539c86`` had
    exited with "No conversation found", leaving a bare shell prompt in
    the pane. A surface-existence probe therefore proves nothing about
    the worker: the seat was alive by that measure and dead in fact, so
    the per-tick sweep never retired it and ``start-lane`` reported
    ``already_running`` for a corpse. A seat is live only when its
    exact managed Claude root process for that exact session is live.

    :func:`live_claude_argv` cannot answer this, because it collapses
    "no such process" and "the measurement failed or was ambiguous"
    into the same empty list. Retirement destroys durable state, so it
    needs those kept apart:

    * ``True``  — ``ps`` succeeded and exactly one managed ``claude``
      process carries this session id. Definitively alive.
    * ``False`` — ``ps`` succeeded and ZERO matched. This is the only
      definitive absence, and the only answer a caller may retire on.
    * ``None``  — unknown, THEREFORE TREAT AS ALIVE. Either ``ps``
      raised (``OSError``/``SubprocessError``), or two or more
      processes matched and no single root can be identified. Retiring
      on an unknown would tear down healthy seats every time ``ps``
      hiccups — on every tick, across every seat at once — so an
      unknown must always fail closed toward leaving the seat alone.
    """

    matches = _managed_claude_process_lines(session_id)
    if matches is None:
        return None
    if len(matches) == 1:
        return True
    if not matches:
        return False
    return None


class ChannelTrustConfirmer:
    """Bounded, single-shot channel-trust trigger for one managed seat.

    Sol correction f0a5a403 (packet 4): the
    :class:`~hermes_orchestrator.channel_trust.ChannelTrustGate` was
    previously reachable only through the manually invoked
    ``channel-trust-confirm`` CLI command, so a newly seated trusted
    launch still required a human. This collaborator is composed by
    ``open_runtime`` and injected into :class:`CmuxLeadSeater`, which
    triggers it exactly once for each newly created channel-launched
    binding: one bounded watch for the development-channel dialog on
    the exact bound surface, then one gate evaluation. The gate is
    reused as-is — claim-before-Enter CAS, mismatch refusal, and
    explicit ambiguous outcomes all hold, so repeated or concurrent
    triggers for one launch (this trigger and the CLI included) still
    send at most one Enter. A watcher timeout is an absent dialog, not
    a trust refusal: zero keys and no durable trust receipt.

    Sol correction a9cc6d5f (packet 3): the watch snapshot is only the
    trigger and initial evidence — the gate's ``read_screen``
    collaborator is wired to a live bounded re-read of the exact bound
    surface, so the gate's last-moment verify re-reads that surface
    immediately before the Enter and sends no key when the dialog is
    no longer freshly, uniquely, and identically present.
    """

    def __init__(
        self,
        *,
        database: Database,
        events: EventStore,
        control: ControlOperations,
        port: CmuxControlPort,
        entry_resolver: Callable[[], Path],
        live_argv: Callable[[str], list[str]] | None = None,
        wait_seconds: int = 90,
        poll_seconds: float = 2.0,
        clock: Callable[[], float] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self._database = database
        self._events = events
        self._control = control
        self._port = port
        self._entry_resolver = entry_resolver
        self._live_argv = live_argv if live_argv is not None else live_claude_argv
        self._wait_seconds = wait_seconds
        self._poll_seconds = poll_seconds
        self._clock = clock if clock is not None else time.monotonic
        self._sleep = sleep if sleep is not None else asyncio.sleep

    async def confirm_seat(self, binding: CmuxBinding) -> TrustVerdict | None:
        """Watch for the dialog on the exact bound surface, then run
        one gate evaluation. ``None`` means the dialog never appeared
        within the bounded window — nothing was pressed and no durable
        trust receipt exists; any pressed-or-refused outcome is the
        gate's own :class:`TrustVerdict` with its durable receipt."""

        if binding.cell_id is None or binding.session_id is None:
            return None
        session_id = str(binding.session_id)
        ref = binding.ref
        # Bounded watch, mirroring the CLI's pattern: the dialog either
        # appears within the window or this trigger does nothing at all.
        deadline = self._clock() + max(1, self._wait_seconds)
        screen = ""
        while True:
            try:
                screen = await self._port.read_screen(ref, lines=200)
            except CmuxError:
                screen = ""
            if (
                "Loading development channels" in screen
                and CHANNEL_ENTRY in screen
            ):
                break
            if self._clock() >= deadline:
                return None
            await self._sleep(self._poll_seconds)
        entry_path = self._entry_resolver()
        launch_argv = self._live_argv(session_id)

        # INFRA-208: a managed rotation seats the replacement lead with
        # a new session/surface (and, on a generation bump, a new
        # canonical entry path) — the exact facts the gate below
        # requires to match the anchor exactly. Carry an already-proven
        # anchor forward to this seat's live identity BEFORE the gate
        # runs, so the rotated seat can still auto-confirm; a rotation
        # this trigger never sees (no anchor, or one that already
        # matches) never attempts a rebind. Any refusal — content
        # drift, a still-pending predecessor's prompt evidence, or a
        # concurrent change — is a genuinely new trust decision, not a
        # rotation carry-forward: it is recorded and this call falls
        # through to evaluate() unchanged, so the existing manual-
        # dialog fallback and its receipts stay exactly as they are.
        anchors = ChannelTrustAnchors(self._database, events=self._events)
        canonical_session = str(uuid.UUID(session_id))
        active_anchor = anchors.active_for_cell(str(binding.cell_id))
        if active_anchor is not None and (
            active_anchor.session_id != canonical_session
            or active_anchor.surface_uuid != binding.surface_uuid
        ):
            try:
                anchors.rebind(
                    cell_id=str(binding.cell_id),
                    profile_alias=str(binding.profile_alias),
                    entry_path=entry_path,
                    package_root=entry_path.parents[2],
                    channel_entry=CHANNEL_ENTRY,
                    launch_argv_template=launch_argv,
                    workspace_uuid=binding.workspace_uuid,
                    surface_uuid=binding.surface_uuid,
                    session_id=session_id,
                )
            except Exception as error:
                with suppress(Exception):
                    self._control.record(
                        kind="channel.rebind_refused",
                        project_key=binding.project_key or "",
                        cell_id=str(binding.cell_id),
                        session_id=session_id,
                        result={"error": str(error)[:200]},
                        reason=(
                            "CHANNEL REBIND REFUSED: the rotated seat's "
                            "trust anchor could not be carried forward "
                            "automatically; the operator must confirm the "
                            "development-channel dialog manually for this "
                            "seat"
                        ),
                    )
        elif active_anchor is None:
            # INFRA-198 blocker 1 (observed live 2026-09-01):
            # ``start-lane`` created visible harness cell 8369559d with
            # no anchor of its own, and every existing anchor belonged
            # to the development cell — so ``anchor_present`` refused on
            # every attempt with no path to ever succeed. ``capture``
            # records a MANUAL trust event, so nothing here may mint one.
            #
            # What the operator proved is a PROGRAM identity, not a
            # property of a cell, so a sibling cell's already-proven
            # anchor is carried onto this one BEFORE the gate runs.
            # ``adopt`` re-measures every content fact and re-checks the
            # argv against the trusted template, so a drifted or
            # unproven candidate is refused rather than trusted. Exactly
            # as with the rotation carry-forward above, a refusal is a
            # genuinely new trust decision: it is recorded and this call
            # falls through to evaluate() unchanged, leaving the manual-
            # dialog fallback and its receipts as they are.
            #
            # INFRA-187 (reopened): when this project has no proven
            # sibling of its own (a brand-new project's first cell),
            # ``proven_source_cell`` widens to a proven anchor held by an
            # active cell of ANOTHER project — still only ever a
            # candidate name; ``adopt`` decides.
            source_cell_id = anchors.proven_source_cell(
                binding.project_key or "", exclude_cell_id=str(binding.cell_id)
            )
            if source_cell_id is not None:
                try:
                    anchors.adopt(
                        source_cell_id=source_cell_id,
                        cell_id=str(binding.cell_id),
                        profile_alias=str(binding.profile_alias),
                        entry_path=entry_path,
                        package_root=entry_path.parents[2],
                        channel_entry=CHANNEL_ENTRY,
                        launch_argv_template=launch_argv,
                        workspace_uuid=binding.workspace_uuid,
                        surface_uuid=binding.surface_uuid,
                        session_id=session_id,
                    )
                except Exception as error:
                    with suppress(Exception):
                        source_project_key = None
                        source_row = self._database.execute(
                            "SELECT project_key FROM project_cells "
                            "WHERE cell_id = ?",
                            (source_cell_id,),
                        ).fetchone()
                        if source_row is not None:
                            source_project_key = str(source_row["project_key"])
                        self._control.record(
                            kind="channel.adopt_refused",
                            project_key=binding.project_key or "",
                            cell_id=str(binding.cell_id),
                            session_id=session_id,
                            result={
                                "error": str(error)[:200],
                                "source_cell_id": source_cell_id,
                                "source_project_key": source_project_key,
                            },
                            reason=(
                                "CHANNEL ADOPT REFUSED: this cell has no "
                                "trust anchor and the project's proven "
                                "anchor could not be carried onto it; the "
                                "operator must confirm the development-"
                                "channel dialog manually for this seat"
                            ),
                        )

        def _run_bounded(operation: Callable[[], Awaitable[Any]]) -> Any:
            # The gate calls its collaborators synchronously between
            # its durable claim and its completion receipt, while this
            # coroutine's own event loop is blocked inside
            # ``gate.evaluate`` — so each bounded cmux operation runs
            # its own loop on a short-lived helper thread; any failure
            # re-raises into the gate's fail-closed handling.
            outcome: list[Any] = []
            failure: list[BaseException] = []

            def _runner() -> None:
                try:
                    outcome.append(asyncio.run(operation()))
                except BaseException as error:
                    failure.append(error)

            worker = threading.Thread(target=_runner, daemon=True)
            worker.start()
            worker.join()
            if failure:
                raise failure[0]
            return outcome[0]

        def _read_live() -> str:
            # Sol correction a9cc6d5f (packet 3): the gate's
            # last-moment verify re-reads the EXACT bound surface live
            # immediately before the Enter — never the watch snapshot
            # above, which is only the trigger evidence
            # (``screen_text``). A replaced or vanished surface fails
            # this bounded read and the gate then sends no key.
            return str(
                _run_bounded(lambda: self._port.read_screen(ref, lines=200))
            )

        def _press() -> None:
            # The one non-parameterizable Enter; a failure re-raises
            # into the gate's explicit ambiguous-outcome handling.
            _run_bounded(lambda: self._port.confirm_channel_dialog(ref))

        gate = ChannelTrustGate(
            self._database,
            self._events,
            anchors,
            self._control,
            _read_live,
            _press,
        )
        return gate.evaluate(
            cell_id=str(binding.cell_id),
            session_id=session_id,
            workspace_uuid=binding.workspace_uuid,
            surface_uuid=binding.surface_uuid,
            profile_alias=str(binding.profile_alias),
            entry_path=entry_path,
            package_root=entry_path.parents[2],
            launch_argv=launch_argv,
            screen_text=screen,
        )


class ChannelTrustTrigger(Protocol):
    """The seater-facing shape of :class:`ChannelTrustConfirmer`."""

    async def confirm_seat(self, binding: CmuxBinding) -> TrustVerdict | None: ...


class CmuxLeadSeater:
    """Ensure each confirmed lead session owns exactly one visible seat.

    Called from the dispatch path once a session is confirmed: a cell
    that already holds an active binding for the same session reuses it
    untouched; otherwise one workspace is created and durably bound. The
    profile alias must still resolve to its exact config directory before
    a seat is created — a vanished profile creates nothing.
    """

    def __init__(
        self,
        *,
        bindings: CmuxSurfaceBindings,
        port: CmuxControlPort,
        project_paths: Mapping[str, Path],
        lane_project_paths: Mapping[tuple[str, str], Path] | None = None,
        prompt_files: Mapping[str, Path] | None = None,
        profile_dirs: ProfileDirectory,
        auth_probe: Callable[[str], bool] | None = None,
        channel_launch: ChannelLaunchSource | None = None,
        control: ControlOperations | None = None,
        channel_trust: ChannelTrustTrigger | None = None,
    ) -> None:
        self._bindings = bindings
        self._port = port
        self._project_paths = dict(project_paths)
        # INFRA-219 R5b: lane-keyed checkout overrides, so a harness
        # seat/binding never resolves into the development worktree.
        self._lane_project_paths = dict(lane_project_paths or {})
        self._prompt_files = dict(prompt_files or {})
        self._profile_dirs = profile_dirs
        self._auth_probe = auth_probe
        self._channel_launch = channel_launch
        self._control = control
        self._channel_trust = channel_trust

    async def retire_failed_seat(
        self, *, cell_id: str, session_id: str, reason: str
    ) -> bool:
        """Remove a failed start's seat residue, in the safe order.

        INFRA-214 (observed live 2026-09-01): the two failed harness
        starts left TWO dead visible cmux workspaces plus active binding
        residue, so an immediate retry could not start clean. Marking
        only the durable row closed would HIDE that residue rather than
        remove it — the workspace would still be on screen and the
        session's channel config still on disk.

        Order matters and mirrors the established idiom in
        ``_close_unconfirmed_channel_trust``: close the exact workspace
        FIRST; mark the binding closed only after a confirmed close;
        when the close cannot be confirmed, hold the binding RESIDUAL as
        ownership evidence so a later reconciliation reclaims the exact
        surface rather than leaking it. Channel configuration is cleaned
        last, once the surface it belongs to is gone.

        Returns True when the workspace close was confirmed. Never
        raises: the caller is already on a launch-failure cleanup path.
        """

        binding = self._bindings.active_lead(cell_id)
        closed = False
        if binding is not None:
            try:
                await self._port.close_workspace(binding.workspace_uuid)
            except CmuxError:
                self._bindings.mark_residual(
                    binding.binding_id, reason=f"{reason}_close_uncertain"
                )
            else:
                closed = True
                self._bindings.mark_closed(binding.binding_id, reason=reason)
        # Sol correction d85c374d: cleanup is GATED on the surface
        # actually being gone. An unconfirmed close may have left the
        # workspace alive, and a live surface stripped of its channel
        # configuration is worse than the residue itself: it survives
        # on screen with no way to reach it, and the residual binding
        # that reconciliation relies on can no longer be resolved.
        # Holding the configuration keeps the surface reclaimable.
        #
        # With NO binding there is no surface to retain, so the orphaned
        # configuration is safe -- and correct -- to remove.
        if self._channel_launch is not None and (binding is None or closed):
            with suppress(Exception):
                self._channel_launch.cleanup(session_id)
        return closed

    async def ensure(
        self,
        *,
        project_key: str,
        cell_id: str,
        session_id: str,
        profile_alias: str,
        issue_id: str | None = None,
        classic_command: str | None = None,
        lane_role: str = "development",
    ) -> CmuxBinding | None:
        # The pane runs exactly the sanitized native TUI command for
        # this exact session; anything else is refused before any
        # workspace exists.
        if classic_command is not None and (
            _CLASSIC_COMMAND.fullmatch(classic_command) is None
            or session_id not in classic_command
        ):
            raise CmuxBindingConflict(
                "only the sanitized classic command for this exact "
                "session may run in a lead seat"
            )
        if self._auth_probe is not None and not self._auth_probe(profile_alias):
            # The probe is a read-only `claude auth status` under the
            # leased profile's exact CLAUDE_CONFIG_DIR: it never starts
            # an OAuth flow, and anything short of a logged-in
            # first-party Max account refuses the seat before creation.
            raise SeatAuthRefused(
                f"profile {profile_alias!r} did not prove the intended "
                "first-party Max account"
            )
        existing = self._bindings.active_lead(cell_id)
        if existing is not None:
            if existing.session_id == session_id:
                await self._show_issue(existing, issue_id)
                return existing
            # The cell rotated to a new session: the old seat's exact
            # workspace is closed and its binding retired before any
            # replacement carries the new identity.
            await self._retire_rotated_seat(existing)
        # Any unresolved residual workspace or interrupted write-ahead
        # activation for this cell must be confirmed closed, lost, or
        # aborted first — a replacement seat while a prior workspace is
        # still owned-but-unresolved would let one cell operate two live
        # seats. A cmux failure here propagates and refuses the seat; the
        # unresolved evidence keeps blocking hibernation.
        await _resolve_residual_bindings(
            self._port,
            self._bindings,
            self._bindings.residual_for_cell(cell_id),
        )
        await _resolve_pending_intents(
            self._port,
            self._bindings,
            self._bindings.pending_intents_for_cell(cell_id),
        )
        cwd = self._project_paths.get(project_key)
        if cwd is None:
            return None
        try:
            config_dir = self._profile_dirs.config_dir(profile_alias)
        except (KeyError, ValueError):
            return None
        # Sol correction b4b545f3 (v5): the fakechat seat-command
        # substitution is retired — the hermes-control channel launch
        # below is the one production extension path for a classic
        # seat, with the documented plain-classic fallback.
        channel_launched = False
        prompt_file = self._prompt_files.get(lane_role)
        if classic_command is not None and self._prompt_files:
            if prompt_file is None:
                raise CmuxBindingConflict(
                    f"no classic lead prompt is configured for lane {lane_role!r}"
                )
            classic_command = classic_resume_command(
                session_id,
                resume="--resume" in classic_command,
                prompt_file=prompt_file,
            )
        if classic_command is not None and self._channel_launch is not None:
            # Channel attachment for the new pane: the session-scoped
            # config and capability are generated under the private
            # state directory and only the sanitized extension grammar
            # can carry them. A launcher failure still falls back to
            # the plain classic command — a seat without its channel
            # drains through the Stop-hook poll — but never silently:
            # one durable, actionable channel.blocked receipt records
            # exactly why the channel is absent.
            try:
                config = self._channel_launch.generate(
                    project_key=project_key,
                    cell_id=cell_id,
                    session_id=session_id,
                    profile_alias=profile_alias,
                    generation=self._bindings.next_lead_generation(cell_id),
                )
                classic_command = classic_channel_command(
                    session_id,
                    resume="--resume" in classic_command,
                    channel_config=config,
                    prompt_file=prompt_file,
                )
                channel_launched = True
            except Exception as error:
                if self._control is not None:
                    with suppress(Exception):
                        self._control.record(
                            kind="channel.blocked",
                            project_key=project_key,
                            cell_id=cell_id,
                            session_id=session_id,
                            result={"launcher_error": str(error)[:200]},
                            reason=(
                                "the channel config could not be "
                                "generated for this seat; the lead "
                                "runs channel-less on the Stop-hook "
                                "drain until hooks or the sidecar "
                                "build are repaired"
                            ),
                        )
        bound = await _activate_lead_seat(
            self._port,
            self._bindings,
            project_key=project_key,
            cwd=cwd,
            config_dir=config_dir,
            cell_id=cell_id,
            session_id=session_id,
            profile_alias=profile_alias,
            lane_role=lane_role,
            command=classic_command,
        )
        if classic_command is not None:
            self._bindings.record_classic(bound.binding_id, session_id)
        await self._show_issue(bound, issue_id)
        if channel_launched and self._channel_trust is not None:
            # Sol correction f0a5a403 (packet 4): one bounded, single-
            # shot trust-gate trigger for the exact newly created
            # channel-launched binding — no manual channel-trust-confirm
            # invocation required. The gate's durable claim CAS keeps
            # concurrent or repeated triggers for this one launch at
            # at most one Enter, and no trigger failure may break the
            # already-bound seat. A seat composed without the
            # collaborator behaves exactly as before.
            with suppress(Exception):
                await self._channel_trust.confirm_seat(bound)
        return bound

    async def _retire_rotated_seat(self, existing: CmuxBinding) -> None:
        """Close the rotated-away workspace by exact identity.

        The binding leaves 'active' only after cmux confirms the close.
        An unconfirmed close holds the seat as residual — still owned and
        blocking hibernation — and stops this activation until startup
        reconciliation resolves the residue.
        """

        try:
            await self._port.close_workspace(existing.workspace_uuid)
        except CmuxError:
            self._bindings.mark_residual(
                existing.binding_id,
                reason="session_rotated_close_uncertain",
            )
            raise
        self._bindings.mark_closed(existing.binding_id, reason="session_rotated")
        if self._channel_launch is not None and existing.session_id:
            # The rotated-away session is safely retired: its channel
            # config and capability may now be removed.
            with suppress(Exception):
                self._channel_launch.cleanup(existing.session_id)

    async def _show_issue(self, binding: CmuxBinding, issue_id: str | None) -> None:
        """Display the seat's current issue id; cosmetic, never binding."""

        if issue_id is None:
            return
        try:
            await self._port.set_status(binding.workspace_uuid, "issue", issue_id)
        except CmuxError:
            return


@dataclass(frozen=True, slots=True)
class HibernationDecision:
    """Whether every visible lead seat may hibernate, with any blockers."""

    clear: bool
    blockers: tuple[str, ...] = ()


class SafetyEvidenceSource(Protocol):
    def current(self, cell_id: str, session_id: str) -> object | None: ...


class CmuxHibernationGate:
    """Permit cmux hibernation only for idle, restorable, safe leads.

    cmux's hibernation toggle is app-wide, so clearance requires every
    active lead binding to be provably safe from durable state alone: no
    live process lease for its session (idle), a current checkpoint-safety
    boundary bound to its exact session (protected), and a cell state that
    a resume can restore. Any running, needs-input, or uncheckpointed lead
    blocks hibernation entirely.
    """

    def __init__(
        self,
        *,
        database: Database,
        bindings: CmuxSurfaceBindings,
        safety: SafetyEvidenceSource,
    ) -> None:
        self._database = database
        self._bindings = bindings
        self._safety = safety

    def decide(self) -> HibernationDecision:
        blockers: list[str] = []
        for binding in self._bindings.active():
            if binding.role != "lead":
                continue
            assert binding.cell_id is not None
            assert binding.session_id is not None
            placeholders = ",".join("?" for _ in _RUNNING_LEASE_STATES)
            running = self._database.scalar(
                f"SELECT count(*) FROM process_leases "
                f"WHERE worker_id = ? AND state IN ({placeholders})",
                (binding.session_id, *_RUNNING_LEASE_STATES),
            )
            if int(running or 0):
                blockers.append(f"{binding.cell_id}:running")
                continue
            cell_state = self._database.scalar(
                "SELECT state FROM project_cells WHERE cell_id = ?",
                (binding.cell_id,),
            )
            if str(cell_state or "") != "active":
                blockers.append(f"{binding.cell_id}:unrestorable")
                continue
            evidence = self._safety.current(binding.cell_id, binding.session_id)
            if evidence is None:
                blockers.append(f"{binding.cell_id}:uncheckpointed")
        for binding in self._bindings.residual():
            # A residual workspace is owned but not confirmed closed;
            # app-wide hibernation waits until reconciliation resolves it.
            blockers.append(f"{binding.cell_id}:residual")
        for intent in self._bindings.pending_intents():
            # A pending intent may own a live workspace whose identities
            # were never bound; hibernation waits for its resolution.
            blockers.append(f"{intent.cell_id}:activation_intent")
        return HibernationDecision(clear=not blockers, blockers=tuple(blockers))


class RegistryProfileDirectory:
    """Resolve profile aliases through the validated profile registry."""

    def __init__(self, registry: Any) -> None:
        self._registry = registry

    def config_dir(self, alias: str) -> Path:
        return Path(self._registry.get(alias).config_dir)


class CmuxWakeAnnouncer:
    """Mirror committed lead wakes onto their cmux seats, event-driven.

    Subscribes to the durable wake outbox: each commit schedules one
    metadata-only status/notification push for the wake's bound seat.
    There is no polling and no model involvement, publication failures are
    swallowed (the durable wake row is the truth and the pane is only a
    view), and outside a running event loop the push is skipped entirely.
    """

    def __init__(
        self,
        *,
        bindings: CmuxSurfaceBindings,
        port: CmuxControlPort,
    ) -> None:
        self._bindings = bindings
        self._port = port
        self._tasks: set[asyncio.Task[None]] = set()

    def attach(self, wakes: Any) -> None:
        wakes.subscribe(self._on_commit)

    def _on_commit(self, wake: Any) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        task = loop.create_task(self._publish(wake))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _publish(self, wake: Any) -> None:
        binding = self._bindings.active_lead(str(wake.cell_id))
        if binding is None:
            return
        try:
            await self._port.set_status(
                binding.workspace_uuid,
                "wake",
                f"{wake.kind}:{wake.reason}",
            )
            await self._port.notify(
                binding.workspace_uuid,
                f"{wake.issue_id}: {wake.kind}",
                wake.reason,
            )
        except CmuxError:
            return


class CmuxHibernationDriver:
    """Apply the hibernation gate's decision to cmux, on change only."""

    def __init__(
        self,
        *,
        gate: CmuxHibernationGate,
        port: CmuxControlPort,
        database: Database,
        events: EventStore,
    ) -> None:
        self._gate = gate
        self._port = port
        self._database = database
        self._events = events
        self._last: bool | None = None

    async def tick(self) -> None:
        decision = self._gate.decide()
        if decision.clear == self._last:
            return
        try:
            await self._port.set_hibernation(decision.clear)
        except CmuxError:
            # The toggle stays unacknowledged; the next tick retries with
            # a freshly derived decision.
            return
        self._last = decision.clear
        with self._database.transaction() as connection:
            self._events.append(
                connection,
                EventInput(
                    event_type="cmux.hibernation",
                    aggregate_type="cmux_binding",
                    aggregate_id="hibernation",
                    payload={
                        "enabled": decision.clear,
                        "blockers": list(decision.blockers),
                    },
                ),
            )


def _identity_payload(
    *,
    role: str,
    project_key: str | None,
    cell_id: str | None,
    session_id: str | None,
    profile_alias: str | None,
    ref: CmuxSurfaceRef,
    generation: int,
    lane_role: str = "development",
) -> dict[str, Any]:
    return {
        "role": role,
        "project_key": project_key,
        "cell_id": cell_id,
        "session_id": session_id,
        "profile_alias": profile_alias,
        "workspace_uuid": ref.workspace_uuid,
        "surface_uuid": ref.surface_uuid,
        "generation": generation,
        "lane_role": lane_role,
    }


def _row_to_intent(row: Any) -> CmuxActivationIntent:
    return CmuxActivationIntent(
        intent_id=str(row["intent_id"]),
        project_key=str(row["project_key"]),
        cell_id=str(row["cell_id"]),
        session_id=str(row["session_id"]),
        profile_alias=str(row["profile_alias"]),
        lane_role=str(row["lane_role"]),
        state=str(row["state"]),
        binding_id=(None if row["binding_id"] is None else str(row["binding_id"])),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


def _row_to_binding(row: Any) -> CmuxBinding:
    return CmuxBinding(
        binding_id=str(row["binding_id"]),
        role=str(row["role"]),
        project_key=(None if row["project_key"] is None else str(row["project_key"])),
        cell_id=(None if row["cell_id"] is None else str(row["cell_id"])),
        session_id=(None if row["session_id"] is None else str(row["session_id"])),
        profile_alias=(
            None if row["profile_alias"] is None else str(row["profile_alias"])
        ),
        lane_role=str(row["lane_role"]),
        workspace_uuid=str(row["workspace_uuid"]),
        surface_uuid=str(row["surface_uuid"]),
        generation=int(row["generation"]),
        state=str(row["state"]),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )
