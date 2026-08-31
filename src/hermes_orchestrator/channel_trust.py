"""Durable trust-anchor state machine for the dev-channel confirmation.

INFRA-197 (v5.1 amendment, operator decision
``infra-197-trusted-channel-auto-approval-20260830-v1``): after ONE exact
manual trust event of ONE exact ``hermes-control`` development-channel
build, a narrowly scoped automatic confirmation may stand in for Claude
Code's per-launch development-channel dialog — and nothing broader. A
:class:`ChannelTrustAnchor` durably records every fact that one manual
trust event proved true: the canonical entry path and its owning uid
(no symlink substitution tolerated anywhere on that chain), the exact
packaged content (an entry sha256 plus a deterministic dist-tree
digest), the plugin's own manifest name/version, the fixed channel
entry, the build timestamp, the exact ``claude`` launch argument
template, the exact cmux workspace/surface/session/profile binding, and
the exact confirmation prompt shape.

:class:`ChannelTrustGate` is the fail-closed re-derivation: every bound
field is re-measured live and compared against the active anchor for
the cell, in a fixed order, stopping at the first mismatch. The prompt
evidence is never arbitrary regex and never a caller-chosen marker
set: the ONLY honored matcher is the one derived from the code-owned
:data:`APPROVED_PROMPT_MARKERS` sequence (exact equality of the
derived markers, at capture, complete_prompt, and evaluation alike).
A full match first records a durable at-most-once
``channel.confirm_claimed`` claim (the live-dedup unique index is the
CAS binding anchor + workspace + surface + launch session), then — Sol
correction a9cc6d5f packet 3 — re-reads the exact bound surface and
fully re-validates the approved dialog IMMEDIATELY before the keypress
(any final-boundary anomaly sends no key and records a durable
non-success refusal while the retained claim blocks blind retry), then
calls the injected ``confirm`` collaborator exactly once, then records
the ``channel.auto_confirmed`` completion receipt carrying the match
evidence — or, when the keypress or its receipt fails after the claim,
an explicit durable ``channel.confirm_ambiguous`` state that reports
``confirmed=False`` (every ambiguous outcome is a non-success verdict)
while the retained claim prevents any blind retry. Any mismatch,
drift, ambiguity, missing anchor,
refused claim, or measurement exception fails closed with a durable
``channel.approval_required`` receipt whose reason starts with
``CHANNEL CONFIRMATION REQUIRED`` and never calls ``confirm``. There is no
generic keystroke or prompt-approval capability here: ``confirm`` and
``read_screen`` are opaque callables the caller (the lead, at wiring
time) binds to the exact validated surface — this module never chooses
a target and never imports cmux.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from hermes_orchestrator.control_operations import ControlOperations
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventInput, EventStore

# The fixed hermes-control development-channel entry. Mirrors
# ``hermes_orchestrator.cmux_surfaces.CHANNEL_ENTRY`` by value only —
# this module never imports cmux or anything that does.
CHANNEL_ENTRY = "server:hermes-control"

_DEV_CHANNEL_FLAG = "--dangerously-load-development-channels"
_LEGACY_CHANNELS_FLAG = "--channels"
_MCP_CONFIG_PATH = re.compile(r"^/[A-Za-z0-9._/-]+\.mcp\.json$")

# The one prompt-matcher shape the gate ever honors (Sol corrections
# b4b545f3 packet 4 and f0a5a403 packet 3): the ``re.escape``'d markers
# of :data:`APPROVED_PROMPT_MARKERS` — exactly those literals, in
# exactly that order — joined by this exact bounded gap. Anything else
# (arbitrary caller-chosen marker sets included, even normalized ones
# naming :data:`CHANNEL_ENTRY`) refuses at capture and fails closed at
# evaluation.
PROMPT_MATCHER_GAP = r"[\s\S]{0,4000}?"

# The operator-approved confirmation dialog, as a CODE-OWNED contract:
# the exact four marker literals genuine hermes-control development-
# channel confirmation dialogs produce, in order. This sequence — not
# any caller-supplied evidence — defines the only prompt shape that can
# ever authorize Enter; capture, complete_prompt, and evaluation all
# require exact equality of the derived marker sequence with it.
APPROVED_PROMPT_MARKERS = (
    "Loading development channels",
    CHANNEL_ENTRY,
    "I am using this for local development",
    "Enter to confirm",
)
APPROVED_PROMPT_PATTERN = PROMPT_MATCHER_GAP.join(
    re.escape(marker) for marker in APPROVED_PROMPT_MARKERS
)


class TrustRefused(ValueError):
    """The requested trust-anchor operation violates the state machine's
    contract (wrong channel entry, symlink substitution, an already
    active anchor, and similar)."""


@dataclass(frozen=True, slots=True)
class ChannelTrustAnchor:
    """One durable anchor binding every fact one manual trust event
    proved true for one cell's development-channel launch."""

    anchor_id: str
    cell_id: str
    profile_alias: str
    canonical_entry_path: str
    entry_owner_uid: int
    entry_sha256: str
    dist_tree_sha256: str
    manifest_name: str
    manifest_version: str
    channel_entry: str
    build_mtime: str
    launch_argv_template: tuple[str, ...]
    workspace_uuid: str
    surface_uuid: str
    session_id: str
    prompt_pattern: str | None
    state: str
    created_at: str
    retired_at: str | None


@dataclass(frozen=True, slots=True)
class TrustVerdict:
    """The bounded outcome of one :meth:`ChannelTrustGate.evaluate` call."""

    confirmed: bool
    anchor_id: str | None = None
    first_failure: str | None = None
    receipt_operation_id: str | None = None


def _require_no_symlinks(entry_path: Path, dist_root: Path) -> None:
    """Refuse if ``entry_path`` or any directory between it and
    ``dist_root`` (both ends inclusive) is a symlink.

    Every level is checked with ``lstat`` (via :meth:`Path.is_symlink`),
    never a resolving ``stat`` — a symlinked entry or a symlinked
    intermediate directory refuses even though a resolved stat would
    silently follow it to different bytes. No symlink substitution is
    tolerated anywhere on the chain.
    """

    if dist_root != entry_path and dist_root not in entry_path.parents:
        raise TrustRefused(
            f"{entry_path} is not inside the channel package root {dist_root}"
        )
    current = entry_path
    while True:
        if current.is_symlink():
            raise TrustRefused(
                f"{current} is a symlink; no symlink substitution is tolerated"
            )
        if current == dist_root:
            return
        current = current.parent


def _dist_tree_sha256(root: Path) -> str:
    """A deterministic digest over every file under ``root``.

    Relative paths sort first, then each path plus that file's own
    sha256 hexdigest feeds one running digest — any addition, removal,
    rename, or content rewrite anywhere in the tree changes the result.
    """

    digest = hashlib.sha256()
    relative_paths = sorted(
        str(path.relative_to(root)) for path in root.rglob("*") if path.is_file()
    )
    for relative_path in relative_paths:
        file_sha256 = hashlib.sha256((root / relative_path).read_bytes()).hexdigest()
        digest.update(relative_path.encode())
        digest.update(file_sha256.encode())
    return digest.hexdigest()


def _read_manifest(package_root: Path) -> tuple[str, str]:
    """The exact ``name``/``version`` of the ``package.json`` at the
    channel package root; a missing key or file propagates as-is."""

    manifest = json.loads((package_root / "package.json").read_text(encoding="utf-8"))
    return str(manifest["name"]), str(manifest["version"])


def _unescape_literal(segment: str) -> str | None:
    """The literal text of one ``re.escape``'d segment, or ``None`` on
    a malformed trailing escape. The caller must still round-trip the
    result through ``re.escape`` — that comparison, not this walk, is
    what refuses unescaped metacharacters and non-normalized escapes."""

    characters: list[str] = []
    index = 0
    while index < len(segment):
        if segment[index] == "\\":
            if index + 1 >= len(segment):
                return None
            characters.append(segment[index + 1])
            index += 2
        else:
            characters.append(segment[index])
            index += 1
    return "".join(characters)


def _fixed_prompt_segments(prompt_pattern: str) -> tuple[str, ...] | None:
    """:data:`APPROVED_PROMPT_MARKERS` when ``prompt_pattern`` is the
    approved fixed normalized prompt matcher, ``None`` for anything
    else.

    Every segment between the exact :data:`PROMPT_MATCHER_GAP`
    separators must round-trip through ``re.escape`` as a literal, and
    the derived marker sequence must equal
    :data:`APPROVED_PROMPT_MARKERS` EXACTLY — extra markers, fewer
    markers, different literals, or a different order all return
    ``None``. Caller-chosen marker sets are never honored, and legacy
    stored patterns (arbitrary or even invalid regex) fail this walk
    without ever being compiled — fail closed, never crash.
    """

    segments = prompt_pattern.split(PROMPT_MATCHER_GAP)
    literals: list[str] = []
    for segment in segments:
        literal = _unescape_literal(segment)
        if literal is None or re.escape(literal) != segment:
            return None
        literals.append(literal)
    if tuple(literals) != APPROVED_PROMPT_MARKERS:
        return None
    return tuple(literals)


def _validate_prompt_pattern(prompt_pattern: str) -> None:
    if not prompt_pattern:
        raise TrustRefused("prompt_pattern must be non-empty")
    if _fixed_prompt_segments(prompt_pattern) is None:
        raise TrustRefused(
            "prompt_pattern is not the approved fixed normalized prompt "
            "matcher: the code-owned approved marker sequence "
            f"{APPROVED_PROMPT_MARKERS!r} joined by "
            f"{PROMPT_MATCHER_GAP!r} is required exactly; caller-chosen "
            "marker sets and arbitrary regular-expression evidence are "
            "refused"
        )


_CHANNEL_ENTRY_TOKEN = re.compile(r"server:[A-Za-z0-9._-]+")


def _dialog_region(screen: str) -> str | None:
    """The substring of ``screen`` spanning the displayed confirmation
    dialog: from the first ``"Loading development channels"`` marker
    through the end of the first ``"Enter to confirm"`` marker that
    follows it, inclusive of both boundaries.

    ``None`` when either boundary marker is absent, or the closing
    marker never follows the opening one — callers must treat that as
    "no dialog", never as an empty dialog. Text outside this span —
    most notably an echoed shell launch command sitting above the
    dialog in a full-scrollback capture — is never folded into the
    region.
    """

    start = screen.find(APPROVED_PROMPT_MARKERS[0])
    if start == -1:
        return None
    end_marker = APPROVED_PROMPT_MARKERS[-1]
    end = screen.find(end_marker, start)
    if end == -1:
        return None
    return screen[start : end + len(end_marker)]


def displayed_channel_entries_valid(screen: str) -> bool:
    """Fail-closed structural replacement for counting
    :data:`CHANNEL_ENTRY` across an entire captured screen: exactly
    one channel-entry token (any ``server:<name>`` occurrence) is
    displayed inside the dialog region — the span from
    ``"Loading development channels"`` through ``"Enter to confirm"``,
    see :func:`_dialog_region` — and that one entry is
    :data:`CHANNEL_ENTRY` exactly.

    Returns ``False`` for a missing dialog region, for a second
    displayed ``server:hermes-control`` line, and for any other
    displayed ``server:<name>`` entry alike — every one of those is an
    unexpected development-channel entry and must fail closed. A
    ``CHANNEL_ENTRY`` occurrence outside the region — most notably the
    echoed shell launch command's own
    ``--dangerously-load-development-channels server:hermes-control``
    argument sitting above the dialog — is never counted.
    """

    region = _dialog_region(screen)
    if region is None:
        return False
    return tuple(_CHANNEL_ENTRY_TOKEN.findall(region)) == (CHANNEL_ENTRY,)


def _is_uuid(token: str) -> bool:
    try:
        uuid.UUID(token)
    except ValueError:
        return False
    return True


def _canonical_uuid(value: object) -> str | None:
    """The canonical text of ``value`` when it parses as a UUID, else
    ``None`` — so a malformed durable row can never compare equal."""

    try:
        return str(uuid.UUID(str(value)))
    except ValueError:
        return None


def _channel_entry_appears_once(argv: Sequence[str]) -> bool:
    """Exactly one channel-loading flag, and it is the fixed dev-channel
    extension carrying :data:`CHANNEL_ENTRY` exactly once."""

    dev_flags = [i for i, token in enumerate(argv) if token == _DEV_CHANNEL_FLAG]
    other_flags = [i for i, token in enumerate(argv) if token == _LEGACY_CHANNELS_FLAG]
    if len(dev_flags) != 1 or other_flags:
        return False
    [idx] = dev_flags
    if idx + 1 >= len(argv) or argv[idx + 1] != CHANNEL_ENTRY:
        return False
    return sum(1 for token in argv if token == CHANNEL_ENTRY) == 1


def _argv_matches_template(
    argv: Sequence[str], template: Sequence[str], *, session_id: str
) -> bool:
    """``argv`` must equal ``template`` token for token, except at a
    session-UUID slot (any template token that parses as a UUID) —
    where the live token must itself parse as a UUID equal to
    ``session_id`` — and a config-path slot (any absolute template
    token ending ``.mcp.json``) — where the live token must be an
    absolute, metacharacter-free ``/…/<session_id>.mcp.json`` path for
    that same session."""

    if len(argv) != len(template):
        return False
    for live_token, template_token in zip(argv, template, strict=True):
        if live_token == template_token:
            continue
        if _is_uuid(template_token):
            if not _is_uuid(live_token) or str(uuid.UUID(live_token)) != session_id:
                return False
            continue
        if template_token.startswith("/") and template_token.endswith(".mcp.json"):
            if _MCP_CONFIG_PATH.fullmatch(live_token) is None:
                return False
            if not live_token.endswith(f"/{session_id}.mcp.json"):
                return False
            continue
        return False
    return True


def _row_to_anchor(row: Any) -> ChannelTrustAnchor:
    return ChannelTrustAnchor(
        anchor_id=str(row["anchor_id"]),
        cell_id=str(row["cell_id"]),
        profile_alias=str(row["profile_alias"]),
        canonical_entry_path=str(row["canonical_entry_path"]),
        entry_owner_uid=int(row["entry_owner_uid"]),
        entry_sha256=str(row["entry_sha256"]),
        dist_tree_sha256=str(row["dist_tree_sha256"]),
        manifest_name=str(row["manifest_name"]),
        manifest_version=str(row["manifest_version"]),
        channel_entry=str(row["channel_entry"]),
        build_mtime=str(row["build_mtime"]),
        launch_argv_template=tuple(
            json.loads(str(row["launch_argv_template_json"]))
        ),
        workspace_uuid=str(row["workspace_uuid"]),
        surface_uuid=str(row["surface_uuid"]),
        session_id=str(row["session_id"]),
        prompt_pattern=(
            None if row["prompt_pattern"] is None else str(row["prompt_pattern"])
        ),
        state=str(row["state"]),
        created_at=str(row["created_at"]),
        retired_at=(None if row["retired_at"] is None else str(row["retired_at"])),
    )


class ChannelTrustAnchors:
    """Capture, retire, and read durable channel-trust anchors.

    At most one anchor is ``active`` per cell — the partial unique
    index enforces it durably; :meth:`capture` also checks it up front
    so a violation refuses with a clear message rather than a bare
    ``sqlite3.IntegrityError``.
    """

    def __init__(
        self,
        database: Database,
        *,
        events: EventStore,
        ids: Callable[[], str] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._database = database
        self._events = events
        self._ids = ids or (lambda: uuid.uuid4().hex)
        self._now = now or (lambda: datetime.now(UTC))

    def capture(
        self,
        *,
        cell_id: str,
        profile_alias: str,
        entry_path: Path,
        package_root: Path,
        channel_entry: str,
        launch_argv_template: Sequence[str],
        workspace_uuid: str,
        surface_uuid: str,
        session_id: str,
        prompt_pattern: str | None = None,
    ) -> ChannelTrustAnchor:
        """Record one anchor from a manual trust event's measured facts.

        Every fact is re-measured from disk here — never trusted from
        the caller — except the identifiers (cell/profile/workspace/
        surface) and the launch argv template, which are the caller's
        own record of what it just launched and confirmed.

        ``prompt_pattern`` may be ``None`` when the manual trust event's
        dialog text could not be recovered (e.g. an alternate-screen TUI
        with no scrollback); the anchor still captures durably with the
        prompt evidence pending, and :meth:`complete_prompt` binds it
        exactly once later. The gate refuses (``prompt_evidence_pending``)
        until it is bound.
        """

        if channel_entry != CHANNEL_ENTRY:
            raise TrustRefused(
                f"channel_entry must be exactly {CHANNEL_ENTRY!r}"
            )
        if prompt_pattern is not None:
            _validate_prompt_pattern(prompt_pattern)
        canonical_session = str(uuid.UUID(str(session_id)))
        entry_path = Path(entry_path)
        package_root = Path(package_root)
        if not entry_path.is_absolute():
            raise TrustRefused("entry_path must be absolute")
        _require_no_symlinks(entry_path, package_root)
        if self.active_for_cell(cell_id) is not None:
            raise TrustRefused(
                f"cell {cell_id!r} already has an active channel trust "
                "anchor; retire it first"
            )

        stat = entry_path.stat()
        entry_owner_uid = int(stat.st_uid)
        entry_sha256 = hashlib.sha256(entry_path.read_bytes()).hexdigest()
        dist_tree_sha256 = _dist_tree_sha256(package_root)
        manifest_name, manifest_version = _read_manifest(package_root)
        build_mtime = datetime.fromtimestamp(stat.st_mtime, tz=UTC).isoformat()
        argv_template = tuple(str(token) for token in launch_argv_template)

        anchor_id = self._ids()
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            live = connection.execute(
                "SELECT anchor_id FROM channel_trust_anchors "
                "WHERE cell_id = ? AND state = 'active'",
                (cell_id,),
            ).fetchone()
            if live is not None:
                raise TrustRefused(
                    f"cell {cell_id!r} already has an active channel trust "
                    "anchor; retire it first"
                )
            connection.execute(
                "INSERT INTO channel_trust_anchors("
                "anchor_id, cell_id, profile_alias, canonical_entry_path, "
                "entry_owner_uid, entry_sha256, dist_tree_sha256, "
                "manifest_name, manifest_version, channel_entry, "
                "build_mtime, launch_argv_template_json, workspace_uuid, "
                "surface_uuid, session_id, prompt_pattern, state, "
                "created_at, retired_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "'active', ?, NULL)",
                (
                    anchor_id,
                    cell_id,
                    profile_alias,
                    str(entry_path),
                    entry_owner_uid,
                    entry_sha256,
                    dist_tree_sha256,
                    manifest_name,
                    manifest_version,
                    channel_entry,
                    build_mtime,
                    json.dumps(list(argv_template), sort_keys=True),
                    workspace_uuid,
                    surface_uuid,
                    canonical_session,
                    prompt_pattern,
                    stamp,
                ),
            )
            self._events.append(
                connection,
                EventInput(
                    event_type="channel_trust_anchor.captured",
                    aggregate_type="channel_trust_anchor",
                    aggregate_id=anchor_id,
                    payload={
                        "cell_id": cell_id,
                        "profile_alias": profile_alias,
                        "canonical_entry_path": str(entry_path),
                        "manifest_name": manifest_name,
                        "manifest_version": manifest_version,
                        "session_id": canonical_session,
                    },
                ),
            )
        return self.get(anchor_id)

    def rebind(
        self,
        *,
        cell_id: str,
        profile_alias: str,
        entry_path: Path,
        package_root: Path,
        channel_entry: str,
        launch_argv_template: Sequence[str],
        workspace_uuid: str,
        surface_uuid: str,
        session_id: str,
    ) -> ChannelTrustAnchor:
        """Carry an already-proven anchor forward across a managed
        rotation that changes only the seat's identity/location facts
        (session, surface, workspace, and — on a generation bump — the
        canonical entry path) — never the packaged CONTENT the
        operator's one manual trust event proved true.

        Unlike :meth:`capture`, ``prompt_pattern`` is never a caller
        argument here: the retiring anchor's own bound prompt evidence
        is the only thing that can ever be carried forward, so a
        predecessor whose prompt evidence is still pending
        (``prompt_pattern is None``) refuses outright — there is
        nothing proven to carry.

        Every CONTENT fact re-measured at the new ``entry_path`` /
        ``package_root`` (symlink guard, owner uid, entry sha256,
        dist-tree sha256, manifest name/version, ``channel_entry``)
        must equal the retiring anchor's exactly; any difference is a
        genuinely new trust decision, refused here with zero database
        mutation so the caller's existing manual-dialog fallback is
        the only path forward. ``canonical_entry_path`` and
        ``build_mtime`` are taken from this fresh measurement, never
        the predecessor's.

        The operator-trusted launch composition is a CONTENT fact too
        (Sol correction f7f6471c): ``launch_argv_template`` — the
        replacement seat's live argv — must match the predecessor's
        trusted template under exactly the two bounded substitutions
        the gate itself permits (the session-UUID slot and the
        session-scoped MCP config-path slot, both for ``session_id``).
        The successor persists the PREDECESSOR's template verbatim, so
        the trusted launch baseline never drifts across any number of
        rotations; any other argv difference is refused with zero
        database mutation and the predecessor stays active.

        A managed account rotation legitimately moves the cell to a
        different healthy profile (Sol correction 531484d2), so
        ``profile_alias`` is not required to equal the predecessor's.
        A different profile is honored only when it is provably
        Hermes's own durable selection for this exact seat: the cell's
        single active lead binding — the seat boundary — must name
        exactly this session, profile, workspace, and surface, and its
        bound write-ahead activation intent — the selection Hermes
        committed before any external create — must name the same
        session and profile. No binding, a stale or residual one, a
        binding for another session or surface, a binding minted
        outside the intent path, or an intent that never selected this
        profile is arbitrary profile drift: refused with zero database
        mutation, and the predecessor stays active.

        Idempotent no-op: when the active anchor already binds this
        exact session/surface/workspace/path, it is returned unchanged
        — re-ensuring an unrotated seat is never a new trust decision.

        On success, the predecessor is retired and the successor
        captured atomically in one transaction — a concurrent rebind
        or retire of the same predecessor fails closed, exactly as
        :meth:`capture`'s own live double-check does.
        """

        if channel_entry != CHANNEL_ENTRY:
            raise TrustRefused(
                f"channel_entry must be exactly {CHANNEL_ENTRY!r}"
            )
        canonical_session = str(uuid.UUID(str(session_id)))
        entry_path = Path(entry_path)
        package_root = Path(package_root)
        if not entry_path.is_absolute():
            raise TrustRefused("entry_path must be absolute")
        _require_no_symlinks(entry_path, package_root)

        predecessor = self.active_for_cell(cell_id)
        if predecessor is None:
            raise TrustRefused(
                f"cell {cell_id!r} has no active channel trust anchor to "
                "rebind"
            )
        if predecessor.prompt_pattern is None:
            raise TrustRefused(
                f"anchor {predecessor.anchor_id!r} has no bound "
                "prompt_pattern; trust is not yet fully proven and cannot "
                "be carried forward"
            )

        # Sol correction f7f6471c: profile and launch composition are
        # never rebound from caller input. The replacement's argv must
        # equal the predecessor's trusted template token for token
        # except at the existing bounded session-UUID and session-
        # scoped MCP config-path slots — otherwise drift would become
        # the trusted baseline the gate below compares against. Sol
        # correction 531484d2: a different profile is honored only as
        # Hermes's own durable selection for this exact seat, proven
        # against the active lead binding and its bound activation
        # intent — never against the caller's argument alone.
        selected_binding_id: str | None = None
        selected_intent_id: str | None = None
        if profile_alias != predecessor.profile_alias:
            selected_binding_id, selected_intent_id = self._selected_replacement(
                cell_id,
                session_id=canonical_session,
                profile_alias=profile_alias,
                workspace_uuid=workspace_uuid,
                surface_uuid=surface_uuid,
            )
        replacement_argv = tuple(str(token) for token in launch_argv_template)
        if not _argv_matches_template(
            replacement_argv,
            predecessor.launch_argv_template,
            session_id=canonical_session,
        ):
            raise TrustRefused(
                "the replacement launch argv does not match the retiring "
                f"anchor {predecessor.anchor_id!r}'s trusted launch "
                "template (only the session UUID and the session-scoped "
                "MCP config path may differ); this is a new trust "
                "decision, not a rotation carry-forward"
            )

        if (
            predecessor.session_id == canonical_session
            and predecessor.surface_uuid == surface_uuid
            and predecessor.workspace_uuid == workspace_uuid
            and predecessor.canonical_entry_path == str(entry_path)
        ):
            return predecessor

        stat = entry_path.stat()
        entry_owner_uid = int(stat.st_uid)
        entry_sha256 = hashlib.sha256(entry_path.read_bytes()).hexdigest()
        dist_tree_sha256 = _dist_tree_sha256(package_root)
        manifest_name, manifest_version = _read_manifest(package_root)
        build_mtime = datetime.fromtimestamp(stat.st_mtime, tz=UTC).isoformat()

        if (
            entry_sha256 != predecessor.entry_sha256
            or dist_tree_sha256 != predecessor.dist_tree_sha256
            or manifest_name != predecessor.manifest_name
            or manifest_version != predecessor.manifest_version
            or entry_owner_uid != predecessor.entry_owner_uid
            or channel_entry != predecessor.channel_entry
        ):
            raise TrustRefused(
                "the content re-measured at the new entry path does not "
                f"match the retiring anchor {predecessor.anchor_id!r}; "
                "this is a new trust decision, not a rotation "
                "carry-forward"
            )

        anchor_id = self._ids()
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            # The same live double-check pattern capture() uses: a
            # concurrent retire or rebind of this exact predecessor
            # between the read above and this transaction fails closed
            # rather than silently retiring the wrong row.
            live = connection.execute(
                "SELECT anchor_id FROM channel_trust_anchors "
                "WHERE cell_id = ? AND state = 'active'",
                (cell_id,),
            ).fetchone()
            if live is None or str(live["anchor_id"]) != predecessor.anchor_id:
                raise TrustRefused(
                    f"cell {cell_id!r}'s active anchor changed "
                    "concurrently; refusing to rebind"
                )
            cursor = connection.execute(
                "UPDATE channel_trust_anchors SET state = 'retired', "
                "retired_at = ? WHERE anchor_id = ? AND state = 'active'",
                (stamp, predecessor.anchor_id),
            )
            if cursor.rowcount != 1:
                raise TrustRefused(
                    f"anchor {predecessor.anchor_id!r} is not retireable"
                )
            self._events.append(
                connection,
                EventInput(
                    event_type="channel_trust_anchor.retired",
                    aggregate_type="channel_trust_anchor",
                    aggregate_id=predecessor.anchor_id,
                    payload={},
                ),
            )
            connection.execute(
                "INSERT INTO channel_trust_anchors("
                "anchor_id, cell_id, profile_alias, canonical_entry_path, "
                "entry_owner_uid, entry_sha256, dist_tree_sha256, "
                "manifest_name, manifest_version, channel_entry, "
                "build_mtime, launch_argv_template_json, workspace_uuid, "
                "surface_uuid, session_id, prompt_pattern, state, "
                "created_at, retired_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "'active', ?, NULL)",
                (
                    anchor_id,
                    cell_id,
                    profile_alias,
                    str(entry_path),
                    entry_owner_uid,
                    entry_sha256,
                    dist_tree_sha256,
                    manifest_name,
                    manifest_version,
                    channel_entry,
                    build_mtime,
                    json.dumps(
                        list(predecessor.launch_argv_template), sort_keys=True
                    ),
                    workspace_uuid,
                    surface_uuid,
                    canonical_session,
                    predecessor.prompt_pattern,
                    stamp,
                ),
            )
            self._events.append(
                connection,
                EventInput(
                    event_type="channel_trust_anchor.rebound",
                    aggregate_type="channel_trust_anchor",
                    aggregate_id=anchor_id,
                    payload={
                        "cell_id": cell_id,
                        "profile_alias": profile_alias,
                        "canonical_entry_path": str(entry_path),
                        "session_id": canonical_session,
                        "predecessor_anchor_id": predecessor.anchor_id,
                        "predecessor_profile_alias": predecessor.profile_alias,
                        "selected_binding_id": selected_binding_id,
                        "selected_intent_id": selected_intent_id,
                    },
                ),
            )
        return self.get(anchor_id)

    def _selected_replacement(
        self,
        cell_id: str,
        *,
        session_id: str,
        profile_alias: str,
        workspace_uuid: str,
        surface_uuid: str,
    ) -> tuple[str, str]:
        """Prove ``profile_alias`` is Hermes's durable replacement
        selection for exactly this seat (Sol correction 531484d2).

        Two Hermes-owned rows must agree, neither of which the caller
        supplies: the cell's single active lead binding (the seat
        boundary) naming exactly this session, profile, workspace, and
        surface, and the bound write-ahead activation intent that
        produced it (the selection committed before any external
        create) naming the same session and profile. Returns their ids
        as rebind evidence; any absence or disagreement refuses.
        """

        refusal = (
            f"profile_alias {profile_alias!r} differs from the retiring "
            "anchor's trusted profile and is not the Hermes-selected "
            f"replacement binding for cell {cell_id!r}"
        )
        binding = self._database.execute(
            "SELECT binding_id, session_id, profile_alias, workspace_uuid, "
            "surface_uuid FROM cmux_surface_bindings "
            "WHERE role = 'lead' AND cell_id = ? AND state = 'active'",
            (cell_id,),
        ).fetchone()
        if binding is None:
            raise TrustRefused(f"{refusal}: no active lead seat binding")
        if (
            _canonical_uuid(binding["session_id"]) != session_id
            or str(binding["profile_alias"]) != profile_alias
            or str(binding["workspace_uuid"]) != workspace_uuid
            or str(binding["surface_uuid"]) != surface_uuid
        ):
            raise TrustRefused(
                f"{refusal}: the active lead seat binding names a different "
                "session, profile, workspace, or surface"
            )
        binding_id = str(binding["binding_id"])
        intent = self._database.execute(
            "SELECT intent_id, session_id, profile_alias "
            "FROM cmux_activation_intents "
            "WHERE state = 'bound' AND binding_id = ? AND cell_id = ?",
            (binding_id, cell_id),
        ).fetchone()
        if intent is None:
            raise TrustRefused(
                f"{refusal}: the seat binding has no bound activation intent"
            )
        if (
            _canonical_uuid(intent["session_id"]) != session_id
            or str(intent["profile_alias"]) != profile_alias
        ):
            raise TrustRefused(
                f"{refusal}: the bound activation intent selected a different "
                "session or profile"
            )
        return binding_id, str(intent["intent_id"])

    def retire(self, anchor_id: str) -> ChannelTrustAnchor:
        stamp = self._now().isoformat()
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE channel_trust_anchors SET state = 'retired', "
                "retired_at = ? WHERE anchor_id = ? AND state = 'active'",
                (stamp, anchor_id),
            )
            if cursor.rowcount != 1:
                raise TrustRefused(f"anchor {anchor_id!r} is not retireable")
            self._events.append(
                connection,
                EventInput(
                    event_type="channel_trust_anchor.retired",
                    aggregate_type="channel_trust_anchor",
                    aggregate_id=anchor_id,
                    payload={},
                ),
            )
        return self.get(anchor_id)

    def complete_prompt(
        self, anchor_id: str, prompt_pattern: str
    ) -> ChannelTrustAnchor:
        """Bind the confirmation prompt shape to an anchor, exactly once.

        Refuses if the anchor is not active, already carries a bound
        (non-NULL) ``prompt_pattern``, or the pattern is empty or an
        invalid regular expression. The row is left completely
        untouched in every refusal case.
        """

        _validate_prompt_pattern(prompt_pattern)
        with self._database.transaction() as connection:
            row = connection.execute(
                "SELECT prompt_pattern FROM channel_trust_anchors "
                "WHERE anchor_id = ? AND state = 'active'",
                (anchor_id,),
            ).fetchone()
            if row is None:
                raise TrustRefused(f"anchor {anchor_id!r} is not active")
            if row["prompt_pattern"] is not None:
                raise TrustRefused(
                    f"anchor {anchor_id!r} already has a bound prompt_pattern"
                )
            cursor = connection.execute(
                "UPDATE channel_trust_anchors SET prompt_pattern = ? "
                "WHERE anchor_id = ? AND state = 'active' "
                "AND prompt_pattern IS NULL",
                (prompt_pattern, anchor_id),
            )
            if cursor.rowcount != 1:
                raise TrustRefused(
                    f"anchor {anchor_id!r} prompt_pattern could not be bound"
                )
            self._events.append(
                connection,
                EventInput(
                    event_type="channel_trust_anchor.prompt_completed",
                    aggregate_type="channel_trust_anchor",
                    aggregate_id=anchor_id,
                    payload={"anchor_id": anchor_id},
                ),
            )
        return self.get(anchor_id)

    def active_for_cell(self, cell_id: str) -> ChannelTrustAnchor | None:
        row = self._database.execute(
            "SELECT * FROM channel_trust_anchors "
            "WHERE cell_id = ? AND state = 'active'",
            (cell_id,),
        ).fetchone()
        return None if row is None else _row_to_anchor(row)

    def get(self, anchor_id: str) -> ChannelTrustAnchor:
        row = self._database.execute(
            "SELECT * FROM channel_trust_anchors WHERE anchor_id = ?",
            (anchor_id,),
        ).fetchone()
        if row is None:
            raise KeyError(anchor_id)
        return _row_to_anchor(row)


class _CheckFailed(Exception):
    """Internal signal naming the first failed verification check."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.name = name


class ChannelTrustGate:
    """Re-derive every bound field live and gate the auto-confirmation.

    ``read_screen`` and ``confirm`` are plain synchronous, zero-argument
    callables — the lead binds them to the exact validated cmux surface
    at wiring time (e.g. a partial application of a narrow
    ``read_screen(ref)`` / ``confirm_channel_dialog(ref)`` cmux op via
    ``asyncio.run`` or an existing loop bridge). This gate never chooses
    a target and never imports cmux.
    """

    def __init__(
        self,
        database: Database,
        events: EventStore,
        anchors: ChannelTrustAnchors,
        control: ControlOperations,
        read_screen: Callable[[], str],
        confirm: Callable[[], None],
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._database = database
        self._events = events
        self._anchors = anchors
        self._control = control
        self._read_screen = read_screen
        self._confirm = confirm
        self._now = now or (lambda: datetime.now(UTC))

    def evaluate(
        self,
        *,
        cell_id: str,
        session_id: str,
        workspace_uuid: str,
        surface_uuid: str,
        profile_alias: str,
        entry_path: Path,
        package_root: Path,
        launch_argv: list[str],
        screen_text: str | None = None,
    ) -> TrustVerdict:
        """Confirm only on a full live re-derivation match; fail closed
        on any mismatch, drift, ambiguity, or measurement exception.

        Checks run in a fixed order and stop at the first failure; that
        failure's name is the only thing carried in the refusal
        receipt. Every measurement failure is caught and turned into a
        refusal; only the anchor lookup may propagate an exception.

        The keypress itself is guarded by a durable at-most-once claim
        (``channel.confirm_claimed``): the claim must record BEFORE
        ``confirm`` is called — a live duplicate or a claim-record
        failure means zero keypresses. After the claim, and immediately
        before the Enter, the approved dialog is re-read and fully
        re-validated on the exact bound surface
        (:meth:`_final_boundary_failure`): a final-boundary anomaly
        sends no key and records a durable approval-required refusal
        while the retained claim keeps blocking blind retry. After the
        external action
        either the ``channel.auto_confirmed`` completion or an explicit
        ``channel.confirm_ambiguous`` state is recorded. A failed or
        uncertain keypress is never blindly retried: the live claim
        keeps refusing until an operator acknowledges it.
        """

        project_key = self._project_key_for(cell_id)
        anchor: ChannelTrustAnchor | None = None
        current_check = "anchor_present"
        try:
            anchor = self._anchors.active_for_cell(cell_id)
            if anchor is None:
                raise _CheckFailed(current_check)

            current_check = "canonical_path"
            entry_path = Path(entry_path)
            package_root = Path(package_root)
            _require_no_symlinks(entry_path, package_root)
            if str(entry_path) != anchor.canonical_entry_path:
                raise _CheckFailed(current_check)

            current_check = "owner_uid"
            if int(entry_path.stat().st_uid) != anchor.entry_owner_uid:
                raise _CheckFailed(current_check)

            current_check = "entry_sha256"
            entry_sha256 = hashlib.sha256(entry_path.read_bytes()).hexdigest()
            if entry_sha256 != anchor.entry_sha256:
                raise _CheckFailed(current_check)

            current_check = "dist_tree_sha256"
            dist_tree_sha256 = _dist_tree_sha256(package_root)
            if dist_tree_sha256 != anchor.dist_tree_sha256:
                raise _CheckFailed(current_check)

            current_check = "manifest"
            manifest_name, manifest_version = _read_manifest(package_root)
            if (manifest_name, manifest_version) != (
                anchor.manifest_name,
                anchor.manifest_version,
            ):
                raise _CheckFailed(current_check)

            current_check = "channel_entry_single"
            if not _channel_entry_appears_once(launch_argv):
                raise _CheckFailed(current_check)

            current_check = "argv_template_match"
            canonical_session = str(uuid.UUID(str(session_id)))
            if not _argv_matches_template(
                launch_argv,
                anchor.launch_argv_template,
                session_id=canonical_session,
            ):
                raise _CheckFailed(current_check)

            current_check = "workspace_uuid"
            if workspace_uuid != anchor.workspace_uuid:
                raise _CheckFailed(current_check)

            current_check = "surface_uuid"
            if surface_uuid != anchor.surface_uuid:
                raise _CheckFailed(current_check)

            current_check = "session_id"
            if canonical_session != anchor.session_id:
                raise _CheckFailed(current_check)

            current_check = "profile_alias"
            if profile_alias != anchor.profile_alias:
                raise _CheckFailed(current_check)

            current_check = "build_mtime"
            build_mtime = datetime.fromtimestamp(
                entry_path.stat().st_mtime, tz=UTC
            ).isoformat()
            if build_mtime != anchor.build_mtime:
                raise _CheckFailed(current_check)

            current_check = "prompt_evidence_pending"
            if anchor.prompt_pattern is None:
                raise _CheckFailed(current_check)

            current_check = "prompt_matcher_fixed"
            if _fixed_prompt_segments(anchor.prompt_pattern) is None:
                raise _CheckFailed(current_check)

            current_check = "prompt_match"
            text = self._read_screen() if screen_text is None else screen_text
            matches = list(re.finditer(anchor.prompt_pattern, text))
            if len(matches) != 1:
                raise _CheckFailed(current_check)
            prompt_match_sha256 = hashlib.sha256(
                matches[0].group(0).encode()
            ).hexdigest()
        except _CheckFailed as failed:
            return self._refuse(
                cell_id=cell_id,
                session_id=session_id,
                project_key=project_key,
                first_failure=failed.name,
                anchor_id=(anchor.anchor_id if anchor is not None else None),
            )
        except Exception:
            return self._refuse(
                cell_id=cell_id,
                session_id=session_id,
                project_key=project_key,
                first_failure=current_check,
                anchor_id=(anchor.anchor_id if anchor is not None else None),
            )

        evidence = {
            "anchor_id": anchor.anchor_id,
            "canonical_entry_path": anchor.canonical_entry_path,
            "entry_owner_uid": anchor.entry_owner_uid,
            "entry_sha256": entry_sha256,
            "dist_tree_sha256": dist_tree_sha256,
            "manifest_name": manifest_name,
            "manifest_version": manifest_version,
            "channel_entry": anchor.channel_entry,
            "workspace_uuid": workspace_uuid,
            "surface_uuid": surface_uuid,
            "session_id": canonical_session,
            "profile_alias": profile_alias,
            "build_mtime": build_mtime,
            "prompt_match_sha256": prompt_match_sha256,
        }

        # The durable at-most-once claim: the unique live-dedup index on
        # control_operations is the CAS. The claim binds the exact
        # anchor, workspace, surface, and launch session (this launch's
        # one claude process) plus the exact matched prompt BEFORE any
        # keypress; a live duplicate or a claim-record failure means
        # zero keypresses.
        claim_key = (
            f"channel.confirm:{anchor.anchor_id}:{workspace_uuid}:"
            f"{surface_uuid}:{canonical_session}"
        )
        try:
            claim = self._control.record(
                kind="channel.confirm_claimed",
                project_key=project_key,
                cell_id=cell_id,
                session_id=session_id,
                result={**evidence, "launch_argv": list(launch_argv)},
                dedup_key=claim_key,
            )
        except Exception:
            return self._refuse(
                cell_id=cell_id,
                session_id=session_id,
                project_key=project_key,
                first_failure="confirm_claim_failed",
                anchor_id=anchor.anchor_id,
            )
        if claim is None:
            return self._refuse(
                cell_id=cell_id,
                session_id=session_id,
                project_key=project_key,
                first_failure="confirm_already_claimed",
                anchor_id=anchor.anchor_id,
            )

        # LAST-MOMENT VERIFY-AND-CONFIRM (Sol correction a9cc6d5f,
        # packet 3): Enter is authorized only when the exact approved
        # dialog is freshly present on the exact bound surface at the
        # keypress boundary — proven by one bounded re-read immediately
        # before the press, never by the evidence the claim was
        # evaluated against. No key was sent on any anomaly here, so
        # the definite non-success refusal is recorded while the live
        # claim keeps refusing any blind retry.
        final_failure = self._final_boundary_failure(
            anchor.prompt_pattern, prompt_match_sha256
        )
        if final_failure is not None:
            return self._refuse(
                cell_id=cell_id,
                session_id=session_id,
                project_key=project_key,
                first_failure=final_failure,
                anchor_id=anchor.anchor_id,
            )

        try:
            self._confirm()
        except Exception:
            return self._ambiguous(
                cell_id=cell_id,
                session_id=session_id,
                project_key=project_key,
                anchor_id=anchor.anchor_id,
                claim_operation_id=claim.operation_id,
                stage="keypress",
            )

        operation = None
        with suppress(Exception):
            operation = self._control.record(
                kind="channel.auto_confirmed",
                project_key=project_key,
                cell_id=cell_id,
                session_id=session_id,
                result=evidence,
            )
        if operation is None:
            # The keypress happened but its completion receipt did not
            # record — the outcome is AMBIGUOUS, never reported as
            # success: an explicit durable state is left and the live
            # claim keeps refusing any further Enter.
            return self._ambiguous(
                cell_id=cell_id,
                session_id=session_id,
                project_key=project_key,
                anchor_id=anchor.anchor_id,
                claim_operation_id=claim.operation_id,
                stage="completion_receipt",
            )
        self._journal(
            confirmed=True,
            cell_id=cell_id,
            session_id=session_id,
            anchor_id=anchor.anchor_id,
            first_failure=None,
        )
        return TrustVerdict(
            confirmed=True,
            anchor_id=anchor.anchor_id,
            receipt_operation_id=operation.operation_id,
        )

    def _final_boundary_failure(
        self, prompt_pattern: str, claimed_match_sha256: str
    ) -> str | None:
        """Re-read and fully re-validate the approved dialog at the
        keypress boundary; ``None`` authorizes the one Enter, any
        string names the invariant that broke (and no key is sent).

        The cmux CLI offers no atomic read-and-press, so this is the
        narrowest achievable window: one bounded live re-read through
        the caller-bound ``read_screen`` — bound at wiring time to the
        exact validated workspace/surface, so a replaced or vanished
        surface fails the read itself — re-validated in full: the
        anchor's fixed approved matcher must match the fresh text
        exactly once, and the matched dialog must be byte-identical
        (by sha256) to the one the durable claim was recorded against.

        RESIDUAL TOCTOU WINDOW, stated honestly: between this final
        read returning and the Enter landing on the surface there
        remains one unavoidable gap — the duration of the ``confirm``
        call itself — in which the pane could still mutate. No
        mechanism available to this process can close that gap; this
        check only shrinks it from
        watch-detection-to-keypress down to read-to-keypress.

        Failure names: ``final_read_failed`` (the re-read raised),
        ``final_prompt_missing`` (the prompt disappeared or a replaced
        surface shows other content), ``final_prompt_multiple`` (the
        match is no longer unique), ``final_prompt_drift`` (one match,
        but its content differs from the claimed evaluation's dialog).
        Each records the existing durable approval-required refusal:
        zero keys went out, so the outcome is definite — never
        ambiguous — and the retained claim still prevents blind retry.
        """

        try:
            text = self._read_screen()
        except Exception:
            return "final_read_failed"
        matches = list(re.finditer(prompt_pattern, text))
        if not matches:
            return "final_prompt_missing"
        if len(matches) > 1:
            return "final_prompt_multiple"
        fresh_sha256 = hashlib.sha256(matches[0].group(0).encode()).hexdigest()
        if fresh_sha256 != claimed_match_sha256:
            return "final_prompt_drift"
        return None

    def _refuse(
        self,
        *,
        cell_id: str,
        session_id: str,
        project_key: str,
        first_failure: str,
        anchor_id: str | None,
    ) -> TrustVerdict:
        operation = None
        with suppress(Exception):
            operation = self._control.record(
                kind="channel.approval_required",
                project_key=project_key,
                cell_id=cell_id,
                session_id=session_id,
                result={"first_failure": first_failure},
                reason=(
                    f"CHANNEL CONFIRMATION REQUIRED: {first_failure} check "
                    "did not match the trusted anchor"
                ),
            )
        self._journal(
            confirmed=False,
            cell_id=cell_id,
            session_id=session_id,
            anchor_id=anchor_id,
            first_failure=first_failure,
        )
        return TrustVerdict(
            confirmed=False,
            anchor_id=anchor_id,
            first_failure=first_failure,
            receipt_operation_id=(
                operation.operation_id if operation is not None else None
            ),
        )

    def _ambiguous(
        self,
        *,
        cell_id: str,
        session_id: str,
        project_key: str,
        anchor_id: str,
        claim_operation_id: str,
        stage: str,
    ) -> TrustVerdict:
        """Record the explicit durable ambiguous state after the
        external action: the keypress failed (``stage="keypress"``) or
        its completion receipt did not record
        (``stage="completion_receipt"``). EVERY ambiguous outcome is a
        non-success verdict (``confirmed=False``,
        ``first_failure="confirm_outcome_ambiguous"``). The claim stays
        live, so nothing blindly retries — recovery is an operator
        verifying the surface and acknowledging the claim and this
        state."""

        detail = (
            "the keypress failed"
            if stage == "keypress"
            else "the completion receipt did not record"
        )
        operation = None
        with suppress(Exception):
            operation = self._control.record(
                kind="channel.confirm_ambiguous",
                project_key=project_key,
                cell_id=cell_id,
                session_id=session_id,
                result={
                    "anchor_id": anchor_id,
                    "claim_operation_id": claim_operation_id,
                    "stage": stage,
                },
                reason=(
                    f"CHANNEL CONFIRM AMBIGUOUS: {detail} after the "
                    "durable claim; verify the surface manually and "
                    "acknowledge the claim to recover"
                ),
                dedup_key=f"channel.confirm_ambiguous:{claim_operation_id}",
            )
        first_failure = "confirm_outcome_ambiguous"
        self._journal(
            confirmed=False,
            cell_id=cell_id,
            session_id=session_id,
            anchor_id=anchor_id,
            first_failure=first_failure,
        )
        return TrustVerdict(
            confirmed=False,
            anchor_id=anchor_id,
            first_failure=first_failure,
            receipt_operation_id=(
                operation.operation_id if operation is not None else None
            ),
        )

    def _journal(
        self,
        *,
        confirmed: bool,
        cell_id: str,
        session_id: str,
        anchor_id: str | None,
        first_failure: str | None,
    ) -> None:
        """Best-effort domain-event audit trail alongside the durable
        control-operation receipt, which remains the authoritative
        record regardless of whether this append succeeds."""

        payload: dict[str, object] = {
            "cell_id": cell_id,
            "session_id": session_id,
        }
        if first_failure is not None:
            payload["first_failure"] = first_failure
        with suppress(Exception), self._database.transaction() as connection:
            self._events.append(
                connection,
                EventInput(
                    event_type=(
                        "channel_trust.confirmed"
                        if confirmed
                        else "channel_trust.refused"
                    ),
                    aggregate_type="channel_trust_anchor",
                    aggregate_id=(anchor_id or cell_id),
                    payload=payload,
                ),
            )

    def _project_key_for(self, cell_id: str) -> str:
        row = self._database.execute(
            "SELECT project_key FROM project_cells WHERE cell_id = ?",
            (cell_id,),
        ).fetchone()
        return "" if row is None else str(row["project_key"])
