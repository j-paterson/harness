"""The bounded dev-channel trust-anchor state machine (INFRA-197 v5.1).

Operator decision ``infra-197-trusted-channel-auto-approval-20260830-v1``:
after ONE exact manual trust event of ONE exact hermes-control
development-channel build, a narrowly scoped automatic confirmation may
stand in for Claude Code's per-launch development-channel dialog.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.channel_trust import (
    APPROVED_PROMPT_MARKERS,
    APPROVED_PROMPT_PATTERN,
    CHANNEL_ENTRY,
    ChannelTrustAnchors,
    ChannelTrustGate,
    TrustRefused,
    _dialog_region,
    displayed_channel_entries_valid,
)
from hermes_orchestrator.cmux import CmuxSurfaceRef
from hermes_orchestrator.cmux_surfaces import CmuxSurfaceBindings
from hermes_orchestrator.control_operations import ControlOperations
from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore

NOW = datetime(2026, 8, 30, 12, tzinfo=UTC)

CELL = "cell-trust-1"
PROFILE = "max-a"
WORKSPACE = "11111111-1111-4111-8111-111111111111"
SURFACE = "22222222-2222-4222-8222-222222222222"
SESSION = "33333333-3333-4333-8333-333333333333"
OTHER_UUID = "44444444-4444-4444-8444-444444444444"
# A second seat identity — the live coordinates a managed rotation
# leaves the successor bound to.
WORKSPACE_2 = "55555555-5555-4555-8555-555555555555"
SURFACE_2 = "66666666-6666-4666-8666-666666666666"
SESSION_2 = "77777777-7777-4777-8777-777777777777"
CONFIG_PATH_2 = f"/state/mcp/{SESSION_2}.mcp.json"
CONFIG_PATH = f"/state/mcp/{SESSION}.mcp.json"

# The approved fixed normalized prompt matcher: the code-owned
# four-marker dialog sequence, re.escape'd and joined by the exact
# bounded gap (Sol corrections b4b545f3 and f0a5a403). Transcribed
# independently here and pinned equal to the module's code-owned
# contract below.
PROMPT_GAP = r"[\s\S]{0,4000}?"
PROMPT_MARKERS = (
    "Loading development channels",
    CHANNEL_ENTRY,
    "I am using this for local development",
    "Enter to confirm",
)
PROMPT_PATTERN = PROMPT_GAP.join(re.escape(marker) for marker in PROMPT_MARKERS)


def test_the_approved_marker_sequence_is_the_code_owned_contract() -> None:
    """The module's code-owned approved sequence is exactly the real
    operator-approved four-marker hermes-control dialog (the shape the
    CLI's bounded prompt capture derives), CHANNEL_ENTRY included."""

    assert APPROVED_PROMPT_MARKERS == PROMPT_MARKERS
    assert CHANNEL_ENTRY in APPROVED_PROMPT_MARKERS
    assert APPROVED_PROMPT_PATTERN == PROMPT_PATTERN
DIALOG_TEXT = (
    "Loading development channels\n"
    f"  - {CHANNEL_ENTRY}\n"
    "  [x] I am using this for local development\n"
    "  Press Enter to confirm, Esc to cancel"
)


def _argv(session_id: str = SESSION, config_path: str = CONFIG_PATH) -> list[str]:
    return [
        "claude",
        "--session-id",
        session_id,
        "--dangerously-skip-permissions",
        "--mcp-config",
        config_path,
        "--dangerously-load-development-channels",
        CHANNEL_ENTRY,
    ]


def _expected_dist_tree_sha256(root: Path) -> str:
    """Independent re-implementation of the packet's dist-tree digest
    contract, so the test does not simply echo the module under test."""

    digest = hashlib.sha256()
    relative_paths = sorted(
        str(path.relative_to(root)) for path in root.rglob("*") if path.is_file()
    )
    for relative_path in relative_paths:
        file_sha256 = hashlib.sha256((root / relative_path).read_bytes()).hexdigest()
        digest.update(relative_path.encode())
        digest.update(file_sha256.encode())
    return digest.hexdigest()


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def events(database: Database) -> EventStore:
    return EventStore(database)


@pytest.fixture
def seeded_cell(database: Database) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO project_cells("
            "cell_id, project_key, state, profile_alias, session_id, "
            "created_at, updated_at) VALUES (?, 'demo', 'active', ?, ?, ?, ?)",
            (CELL, PROFILE, SESSION, NOW.isoformat(), NOW.isoformat()),
        )


@pytest.fixture
def control(database: Database, events: EventStore) -> ControlOperations:
    return ControlOperations(database, events=events, now=lambda: NOW)


@pytest.fixture
def anchors(database: Database, events: EventStore) -> ChannelTrustAnchors:
    return ChannelTrustAnchors(database, events=events, now=lambda: NOW)


def _write_package(package_root: Path) -> tuple[Path, Path]:
    """Lay out one channel package (manifest, entry, and one extra
    dist file) under ``package_root``; every call with byte-identical
    file contents produces byte-identical entry/dist-tree digests
    regardless of the path — the content that ``rebind`` requires
    equal is never the path."""

    dist = package_root / "dist"
    (dist / "lib").mkdir(parents=True)
    (package_root / "package.json").write_text(
        json.dumps({"name": "hermes-control-channel", "version": "1.2.3"}),
        encoding="utf-8",
    )
    entry_path = dist / "index.js"
    entry_path.write_text("console.log('hermes-control');\n", encoding="utf-8")
    (dist / "lib" / "util.js").write_text("module.exports = {};\n", encoding="utf-8")
    return package_root, entry_path


@pytest.fixture
def package(tmp_path: Path) -> tuple[Path, Path]:
    return _write_package(tmp_path / "channel-pkg")


def _capture(
    anchors: ChannelTrustAnchors,
    *,
    package_root: Path,
    entry_path: Path,
    prompt_pattern: str | None = PROMPT_PATTERN,
    cell_id: str = CELL,
):
    return anchors.capture(
        cell_id=cell_id,
        profile_alias=PROFILE,
        entry_path=entry_path,
        package_root=package_root,
        channel_entry=CHANNEL_ENTRY,
        launch_argv_template=_argv(),
        workspace_uuid=WORKSPACE,
        surface_uuid=SURFACE,
        session_id=SESSION,
        prompt_pattern=prompt_pattern,
    )


class _Confirm:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self) -> None:
        self.calls += 1


class _ReadScreen:
    """Fails the test if invoked when no text was ever expected to be
    read — most tests pass ``screen_text`` directly to ``evaluate``."""

    def __init__(self, text: str | None = None) -> None:
        self.calls = 0
        self.text = text

    def __call__(self) -> str:
        self.calls += 1
        if self.text is None:
            raise AssertionError("read_screen should not have been called")
        return self.text


def _make_gate(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    *,
    screen_text: str | None = None,
) -> tuple[ChannelTrustGate, _Confirm, _ReadScreen]:
    confirm = _Confirm()
    read_screen = _ReadScreen(screen_text)
    gate = ChannelTrustGate(database, events, anchors, control, read_screen, confirm)
    return gate, confirm, read_screen


def _evaluate(
    gate: ChannelTrustGate,
    *,
    entry_path: Path,
    package_root: Path,
    session_id: str = SESSION,
    workspace_uuid: str = WORKSPACE,
    surface_uuid: str = SURFACE,
    profile_alias: str = PROFILE,
    cell_id: str = CELL,
    argv: list[str] | None = None,
    screen_text: str | None = None,
):
    return gate.evaluate(
        cell_id=cell_id,
        session_id=session_id,
        workspace_uuid=workspace_uuid,
        surface_uuid=surface_uuid,
        profile_alias=profile_alias,
        entry_path=entry_path,
        package_root=package_root,
        launch_argv=_argv() if argv is None else argv,
        screen_text=screen_text,
    )


# --------------------------------------------------------------------
# ChannelTrustAnchors.capture
# --------------------------------------------------------------------


def test_capture_records_every_measured_fact(
    anchors: ChannelTrustAnchors, package: tuple[Path, Path]
) -> None:
    package_root, entry_path = package

    anchor = _capture(anchors, package_root=package_root, entry_path=entry_path)

    assert anchor.state == "active"
    assert anchor.cell_id == CELL
    assert anchor.profile_alias == PROFILE
    assert anchor.canonical_entry_path == str(entry_path)
    assert anchor.entry_owner_uid == entry_path.stat().st_uid
    assert anchor.entry_sha256 == hashlib.sha256(entry_path.read_bytes()).hexdigest()
    assert anchor.dist_tree_sha256 == _expected_dist_tree_sha256(package_root)
    assert anchor.manifest_name == "hermes-control-channel"
    assert anchor.manifest_version == "1.2.3"
    assert anchor.channel_entry == CHANNEL_ENTRY
    assert anchor.launch_argv_template == tuple(_argv())
    assert anchor.workspace_uuid == WORKSPACE
    assert anchor.surface_uuid == SURFACE
    assert anchor.session_id == SESSION
    assert anchor.prompt_pattern == PROMPT_PATTERN
    assert anchor.build_mtime == datetime.fromtimestamp(
        entry_path.stat().st_mtime, tz=UTC
    ).isoformat()
    assert anchors.active_for_cell(CELL) == anchor
    assert anchors.get(anchor.anchor_id) == anchor


def test_capture_prompt_pattern_may_be_none_pending(
    anchors: ChannelTrustAnchors, package: tuple[Path, Path]
) -> None:
    package_root, entry_path = package

    anchor = _capture(
        anchors, package_root=package_root, entry_path=entry_path, prompt_pattern=None
    )

    assert anchor.prompt_pattern is None
    assert anchor.state == "active"


def test_capture_refuses_a_second_active_anchor_for_the_same_cell(
    anchors: ChannelTrustAnchors, package: tuple[Path, Path]
) -> None:
    package_root, entry_path = package
    _capture(anchors, package_root=package_root, entry_path=entry_path)

    with pytest.raises(TrustRefused, match="already has an active"):
        _capture(anchors, package_root=package_root, entry_path=entry_path)


def test_capture_retire_then_recapture_succeeds(
    anchors: ChannelTrustAnchors, package: tuple[Path, Path]
) -> None:
    package_root, entry_path = package
    first = _capture(anchors, package_root=package_root, entry_path=entry_path)

    anchors.retire(first.anchor_id)
    second = _capture(anchors, package_root=package_root, entry_path=entry_path)

    assert second.anchor_id != first.anchor_id
    assert anchors.get(first.anchor_id).state == "retired"
    assert anchors.active_for_cell(CELL) == second


def test_capture_refuses_when_entry_path_is_a_symlink(
    anchors: ChannelTrustAnchors, package: tuple[Path, Path], tmp_path: Path
) -> None:
    package_root, entry_path = package
    real_target = tmp_path / "elsewhere.js"
    real_target.write_text("evil\n", encoding="utf-8")
    entry_path.unlink()
    entry_path.symlink_to(real_target)

    with pytest.raises(TrustRefused, match="symlink"):
        _capture(anchors, package_root=package_root, entry_path=entry_path)


def test_capture_refuses_wrong_channel_entry(
    anchors: ChannelTrustAnchors, package: tuple[Path, Path]
) -> None:
    package_root, entry_path = package

    with pytest.raises(TrustRefused, match="channel_entry"):
        anchors.capture(
            cell_id=CELL,
            profile_alias=PROFILE,
            entry_path=entry_path,
            package_root=package_root,
            channel_entry="plugin:fakechat@claude-plugins-official",
            launch_argv_template=_argv(),
            workspace_uuid=WORKSPACE,
            surface_uuid=SURFACE,
            session_id=SESSION,
            prompt_pattern=PROMPT_PATTERN,
        )


# --------------------------------------------------------------------
# ChannelTrustAnchors.complete_prompt
# --------------------------------------------------------------------


def test_complete_prompt_binds_then_gate_full_matches(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    package_root, entry_path = package
    anchor = _capture(
        anchors, package_root=package_root, entry_path=entry_path, prompt_pattern=None
    )

    completed = anchors.complete_prompt(anchor.anchor_id, PROMPT_PATTERN)
    assert completed.prompt_pattern == PROMPT_PATTERN

    screen_text = f"...\n{DIALOG_TEXT}\n"
    gate, confirm, _ = _make_gate(
        database, events, anchors, control, screen_text=screen_text
    )
    result = _evaluate(
        gate, entry_path=entry_path, package_root=package_root, screen_text=screen_text
    )

    assert result.confirmed is True
    assert confirm.calls == 1


def test_complete_prompt_refuses_a_second_bind(
    anchors: ChannelTrustAnchors, package: tuple[Path, Path]
) -> None:
    package_root, entry_path = package
    anchor = _capture(
        anchors, package_root=package_root, entry_path=entry_path, prompt_pattern=None
    )
    anchors.complete_prompt(anchor.anchor_id, PROMPT_PATTERN)

    with pytest.raises(TrustRefused, match="already has a bound"):
        anchors.complete_prompt(anchor.anchor_id, PROMPT_PATTERN)


def test_complete_prompt_refuses_a_non_fixed_matcher(
    anchors: ChannelTrustAnchors, package: tuple[Path, Path]
) -> None:
    package_root, entry_path = package
    anchor = _capture(
        anchors, package_root=package_root, entry_path=entry_path, prompt_pattern=None
    )

    with pytest.raises(TrustRefused, match="fixed normalized"):
        anchors.complete_prompt(anchor.anchor_id, "(unterminated[")


# --------------------------------------------------------------------
# ChannelTrustAnchors.rebind
# --------------------------------------------------------------------


def test_rebind_retires_predecessor_and_captures_successor_atomically(
    database: Database,
    anchors: ChannelTrustAnchors,
    package: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    package_root, entry_path = package
    predecessor = _capture(anchors, package_root=package_root, entry_path=entry_path)

    # A rotation that also bumped the runtime generation: a new
    # canonical path, byte-identical content.
    rotated_root, rotated_entry = _write_package(tmp_path / "channel-pkg-gen2")

    successor = anchors.rebind(
        cell_id=CELL,
        profile_alias=PROFILE,
        entry_path=rotated_entry,
        package_root=rotated_root,
        channel_entry=CHANNEL_ENTRY,
        launch_argv_template=_argv(SESSION_2, CONFIG_PATH_2),
        workspace_uuid=WORKSPACE_2,
        surface_uuid=SURFACE_2,
        session_id=SESSION_2,
    )

    assert successor.anchor_id != predecessor.anchor_id
    # The predecessor's prompt evidence is carried forward exactly —
    # rebind never re-proves the prompt shape.
    assert successor.prompt_pattern == predecessor.prompt_pattern
    # Identity/location facts are all rebound to the live seat.
    assert successor.canonical_entry_path == str(rotated_entry)
    assert successor.build_mtime == datetime.fromtimestamp(
        rotated_entry.stat().st_mtime, tz=UTC
    ).isoformat()
    assert successor.session_id == SESSION_2
    assert successor.workspace_uuid == WORKSPACE_2
    assert successor.surface_uuid == SURFACE_2
    # The trusted launch template is carried forward verbatim (Sol
    # correction f7f6471c): the replacement's argv only had to MATCH
    # it under the bounded session substitutions — it never becomes
    # the baseline.
    assert successor.launch_argv_template == predecessor.launch_argv_template
    assert successor.launch_argv_template == tuple(_argv())
    assert successor.profile_alias == predecessor.profile_alias
    # Content facts are unchanged — that equality is what authorized
    # the carry-forward.
    assert successor.entry_sha256 == predecessor.entry_sha256
    assert successor.dist_tree_sha256 == predecessor.dist_tree_sha256
    assert successor.manifest_name == predecessor.manifest_name
    assert successor.manifest_version == predecessor.manifest_version
    assert successor.entry_owner_uid == predecessor.entry_owner_uid
    assert successor.state == "active"

    assert anchors.get(predecessor.anchor_id).state == "retired"
    assert anchors.active_for_cell(CELL) == successor

    rows = database.execute(
        "SELECT event_type, aggregate_id, payload_json FROM events "
        "WHERE aggregate_type = 'channel_trust_anchor' ORDER BY sequence"
    ).fetchall()
    assert [str(row["event_type"]) for row in rows] == [
        "channel_trust_anchor.captured",
        "channel_trust_anchor.retired",
        "channel_trust_anchor.rebound",
    ]
    assert str(rows[1]["aggregate_id"]) == predecessor.anchor_id
    rebound_payload = json.loads(str(rows[2]["payload_json"]))
    assert rebound_payload["predecessor_anchor_id"] == predecessor.anchor_id
    assert rebound_payload["cell_id"] == CELL
    assert rebound_payload["profile_alias"] == PROFILE
    assert rebound_payload["canonical_entry_path"] == str(rotated_entry)
    assert rebound_payload["session_id"] == SESSION_2


def test_rebind_refuses_on_content_digest_mismatch_with_zero_mutation(
    database: Database,
    anchors: ChannelTrustAnchors,
    package: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    package_root, entry_path = package
    predecessor = _capture(anchors, package_root=package_root, entry_path=entry_path)

    drifted_root, drifted_entry = _write_package(tmp_path / "channel-pkg-drifted")
    # A genuinely different build at the new location — not the same
    # content the operator trusted.
    drifted_entry.write_text("console.log('different!');\n", encoding="utf-8")

    before = database.execute(
        "SELECT anchor_id, state FROM channel_trust_anchors ORDER BY anchor_id"
    ).fetchall()

    with pytest.raises(TrustRefused, match="new trust decision"):
        anchors.rebind(
            cell_id=CELL,
            profile_alias=PROFILE,
            entry_path=drifted_entry,
            package_root=drifted_root,
            channel_entry=CHANNEL_ENTRY,
            launch_argv_template=_argv(SESSION_2, CONFIG_PATH_2),
            workspace_uuid=WORKSPACE_2,
            surface_uuid=SURFACE_2,
            session_id=SESSION_2,
        )

    after = database.execute(
        "SELECT anchor_id, state FROM channel_trust_anchors ORDER BY anchor_id"
    ).fetchall()
    assert [(str(row["anchor_id"]), str(row["state"])) for row in before] == [
        (str(row["anchor_id"]), str(row["state"])) for row in after
    ]
    assert anchors.get(predecessor.anchor_id).state == "active"
    assert anchors.active_for_cell(CELL) == predecessor


def test_rebind_refuses_with_no_active_anchor(
    anchors: ChannelTrustAnchors, package: tuple[Path, Path]
) -> None:
    package_root, entry_path = package

    with pytest.raises(TrustRefused, match="no active channel trust anchor"):
        anchors.rebind(
            cell_id=CELL,
            profile_alias=PROFILE,
            entry_path=entry_path,
            package_root=package_root,
            channel_entry=CHANNEL_ENTRY,
            launch_argv_template=_argv(SESSION_2, CONFIG_PATH_2),
            workspace_uuid=WORKSPACE_2,
            surface_uuid=SURFACE_2,
            session_id=SESSION_2,
        )


def test_rebind_refuses_a_predecessor_with_prompt_evidence_pending(
    anchors: ChannelTrustAnchors, package: tuple[Path, Path]
) -> None:
    package_root, entry_path = package
    _capture(
        anchors, package_root=package_root, entry_path=entry_path, prompt_pattern=None
    )

    with pytest.raises(TrustRefused, match="not yet fully proven"):
        anchors.rebind(
            cell_id=CELL,
            profile_alias=PROFILE,
            entry_path=entry_path,
            package_root=package_root,
            channel_entry=CHANNEL_ENTRY,
            launch_argv_template=_argv(SESSION_2, CONFIG_PATH_2),
            workspace_uuid=WORKSPACE_2,
            surface_uuid=SURFACE_2,
            session_id=SESSION_2,
        )


def test_rebind_is_idempotent_for_an_already_matching_active_anchor(
    database: Database,
    anchors: ChannelTrustAnchors,
    package: tuple[Path, Path],
) -> None:
    package_root, entry_path = package
    anchor = _capture(anchors, package_root=package_root, entry_path=entry_path)

    returned = anchors.rebind(
        cell_id=CELL,
        profile_alias=PROFILE,
        entry_path=entry_path,
        package_root=package_root,
        channel_entry=CHANNEL_ENTRY,
        launch_argv_template=_argv(),
        workspace_uuid=WORKSPACE,
        surface_uuid=SURFACE,
        session_id=SESSION,
    )

    assert returned == anchor
    assert anchors.get(anchor.anchor_id).state == "active"
    rows = database.execute(
        "SELECT event_type FROM events "
        "WHERE aggregate_type = 'channel_trust_anchor' ORDER BY sequence"
    ).fetchall()
    # No extra events — an already-matching call is not a new trust
    # decision.
    assert [str(row["event_type"]) for row in rows] == [
        "channel_trust_anchor.captured"
    ]


# --------------------------------------------------------------------
# ChannelTrustAnchors.rebind — profile / launch-template continuity
# (Sol correction f7f6471c)
# --------------------------------------------------------------------

# A third seat identity for chained-rotation and drift cases.
SESSION_3 = "88888888-8888-4888-8888-888888888888"
CONFIG_PATH_3 = f"/state/mcp/{SESSION_3}.mcp.json"

# The different healthy profile a managed account rotation selects
# (Sol correction 531484d2) — never equal to the anchor's own.
SELECTED_PROFILE = "max-b"
assert SELECTED_PROFILE != PROFILE


def _seat_replacement(
    database: Database,
    *,
    session_id: str = SESSION_2,
    profile_alias: str = SELECTED_PROFILE,
    workspace_uuid: str = WORKSPACE_2,
    surface_uuid: str = SURFACE_2,
    via_intent: bool = True,
    activate: bool = True,
    cell_id: str = CELL,
):
    """Seat a replacement lead the way Hermes does: a write-ahead
    activation intent naming the selected session/profile, bound to
    the returned workspace as a residual row, then promoted to the
    cell's single active binding. ``via_intent=False`` mints a bare
    binding outside the intent path; ``activate=False`` leaves the
    bound row residual."""

    bindings = CmuxSurfaceBindings(database=database, events=EventStore(database))
    ref = CmuxSurfaceRef(workspace_uuid=workspace_uuid, surface_uuid=surface_uuid)
    if not via_intent:
        return bindings.bind_lead(
            project_key="demo",
            cell_id=cell_id,
            session_id=session_id,
            profile_alias=profile_alias,
            ref=ref,
        )
    intent = bindings.record_intent(
        project_key="demo",
        cell_id=cell_id,
        session_id=session_id,
        profile_alias=profile_alias,
    )
    bound = bindings.bind_intent(intent.intent_id, ref=ref)
    if not activate:
        return bound
    return bindings.activate_residual(bound.binding_id)


def _anchor_rows(database: Database) -> list[tuple[str, str, str, str]]:
    rows = database.execute(
        "SELECT anchor_id, state, profile_alias, launch_argv_template_json "
        "FROM channel_trust_anchors ORDER BY anchor_id"
    ).fetchall()
    return [
        (
            str(row["anchor_id"]),
            str(row["state"]),
            str(row["profile_alias"]),
            str(row["launch_argv_template_json"]),
        )
        for row in rows
    ]


def _anchor_event_types(database: Database) -> list[str]:
    rows = database.execute(
        "SELECT event_type FROM events "
        "WHERE aggregate_type = 'channel_trust_anchor' ORDER BY sequence"
    ).fetchall()
    return [str(row["event_type"]) for row in rows]


def _rebind_rotated(
    anchors: ChannelTrustAnchors,
    *,
    package_root: Path,
    entry_path: Path,
    profile_alias: str = PROFILE,
    launch_argv_template: list[str] | None = None,
    session_id: str = SESSION_2,
):
    return anchors.rebind(
        cell_id=CELL,
        profile_alias=profile_alias,
        entry_path=entry_path,
        package_root=package_root,
        channel_entry=CHANNEL_ENTRY,
        launch_argv_template=(
            _argv(SESSION_2, CONFIG_PATH_2)
            if launch_argv_template is None
            else launch_argv_template
        ),
        workspace_uuid=WORKSPACE_2,
        surface_uuid=SURFACE_2,
        session_id=session_id,
    )


def test_rebind_refuses_a_profile_mismatch_with_zero_mutation(
    database: Database,
    anchors: ChannelTrustAnchors,
    package: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    """Byte-identical content at the new path, a well-formed rotated
    argv — but a different profile alias that no durable Hermes
    replacement selection names. The operator trusted ONE profile; a
    caller argument alone may not re-home trust to another."""

    package_root, entry_path = package
    predecessor = _capture(anchors, package_root=package_root, entry_path=entry_path)
    rotated_root, rotated_entry = _write_package(tmp_path / "channel-pkg-gen2")
    before = _anchor_rows(database)

    with pytest.raises(TrustRefused, match="Hermes-selected replacement binding"):
        _rebind_rotated(
            anchors,
            package_root=rotated_root,
            entry_path=rotated_entry,
            profile_alias="max-z",
        )

    assert _anchor_rows(database) == before
    assert anchors.get(predecessor.anchor_id).state == "active"
    assert anchors.active_for_cell(CELL) == predecessor
    assert _anchor_event_types(database) == ["channel_trust_anchor.captured"]


def _drift_argv(mutate) -> list[str]:
    argv = _argv(SESSION_2, CONFIG_PATH_2)
    return mutate(argv)


@pytest.mark.parametrize(
    "drifted_argv",
    [
        pytest.param(
            _drift_argv(lambda a: [*a, "--verbose"]),
            id="extra-flag-appended",
        ),
        pytest.param(
            _drift_argv(lambda a: [*a[:3], "--add-dir", "/", *a[3:]]),
            id="extra-flag-inserted",
        ),
        pytest.param(
            _drift_argv(
                lambda a: [t for t in a if t != "--dangerously-skip-permissions"]
            ),
            id="fixed-flag-dropped",
        ),
        pytest.param(
            _drift_argv(lambda a: [*a[:3], "--permission-mode=bypass", *a[4:]]),
            id="fixed-flag-substituted",
        ),
        pytest.param(
            _drift_argv(lambda a: ["/usr/local/bin/claude", *a[1:]]),
            id="executable-token-changed",
        ),
        pytest.param(
            _argv(SESSION_3, CONFIG_PATH_2),
            id="session-uuid-slot-is-another-session",
        ),
        pytest.param(
            _argv(SESSION_2, CONFIG_PATH_3),
            id="config-path-is-another-sessions",
        ),
        pytest.param(
            _argv(SESSION_2, f"/state/mcp/{SESSION_2}.json"),
            id="config-path-not-mcp-json",
        ),
        pytest.param(
            _argv(SESSION_2, f"/state/mcp/$(x)/{SESSION_2}.mcp.json"),
            id="config-path-with-metacharacter",
        ),
        pytest.param(
            _argv(SESSION_2, f"state/mcp/{SESSION_2}.mcp.json"),
            id="config-path-relative",
        ),
        pytest.param(
            _drift_argv(
                lambda a: [*a, "--dangerously-load-development-channels", CHANNEL_ENTRY]
            ),
            id="second-channel-entry",
        ),
        pytest.param([], id="empty-argv"),
    ],
)
def test_rebind_refuses_arbitrary_launch_argument_drift_with_zero_mutation(
    database: Database,
    anchors: ChannelTrustAnchors,
    package: tuple[Path, Path],
    tmp_path: Path,
    drifted_argv: list[str],
) -> None:
    """Same profile, byte-identical content — but the replacement's
    argv differs from the trusted template somewhere OTHER than the two
    bounded session slots. Refused before anything is measured or
    retired: the predecessor stays active and no row or event moves."""

    package_root, entry_path = package
    predecessor = _capture(anchors, package_root=package_root, entry_path=entry_path)
    rotated_root, rotated_entry = _write_package(tmp_path / "channel-pkg-gen2")
    before = _anchor_rows(database)

    with pytest.raises(TrustRefused, match="trusted launch template"):
        _rebind_rotated(
            anchors,
            package_root=rotated_root,
            entry_path=rotated_entry,
            launch_argv_template=drifted_argv,
        )

    assert _anchor_rows(database) == before
    assert anchors.get(predecessor.anchor_id).state == "active"
    assert anchors.active_for_cell(CELL) == predecessor
    assert _anchor_event_types(database) == ["channel_trust_anchor.captured"]


def test_rebind_refuses_drift_before_the_idempotent_no_op(
    database: Database,
    anchors: ChannelTrustAnchors,
    package: tuple[Path, Path],
) -> None:
    """Even a call that names the anchor's OWN seat exactly is refused
    when it arrives with a different profile or launch composition —
    drift is never a no-op, it is a refusal with zero mutation."""

    package_root, entry_path = package
    anchor = _capture(anchors, package_root=package_root, entry_path=entry_path)
    before = _anchor_rows(database)

    with pytest.raises(TrustRefused, match="Hermes-selected replacement binding"):
        anchors.rebind(
            cell_id=CELL,
            profile_alias="max-z",
            entry_path=entry_path,
            package_root=package_root,
            channel_entry=CHANNEL_ENTRY,
            launch_argv_template=_argv(),
            workspace_uuid=WORKSPACE,
            surface_uuid=SURFACE,
            session_id=SESSION,
        )
    with pytest.raises(TrustRefused, match="trusted launch template"):
        anchors.rebind(
            cell_id=CELL,
            profile_alias=PROFILE,
            entry_path=entry_path,
            package_root=package_root,
            channel_entry=CHANNEL_ENTRY,
            launch_argv_template=[*_argv(), "--verbose"],
            workspace_uuid=WORKSPACE,
            surface_uuid=SURFACE,
            session_id=SESSION,
        )

    assert _anchor_rows(database) == before
    assert anchors.active_for_cell(CELL) == anchor
    assert _anchor_event_types(database) == ["channel_trust_anchor.captured"]


def test_rebind_carries_trust_to_the_hermes_selected_replacement_profile(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
    tmp_path: Path,
) -> None:
    """Sol correction 531484d2: a realistic managed account rotation —
    the anchor was trusted on profile A, Hermes selected a different
    healthy profile B for the replacement and durably seated it (write-
    ahead intent → bound residual → active lead binding), and the
    replacement's argv matches the trusted template under the bounded
    session substitutions. The rebind honors B because the durable seat
    proves it was Hermes's selection, carries content + prompt +
    template forward unchanged, and the gate then auto-confirms the
    rotated seat under B with exactly one Enter."""

    package_root, entry_path = package
    predecessor = _capture(anchors, package_root=package_root, entry_path=entry_path)
    rotated_root, rotated_entry = _write_package(tmp_path / "channel-pkg-gen2")
    seat = _seat_replacement(database)

    successor = _rebind_rotated(
        anchors,
        package_root=rotated_root,
        entry_path=rotated_entry,
        profile_alias=SELECTED_PROFILE,
    )

    assert successor.profile_alias == SELECTED_PROFILE
    assert successor.session_id == SESSION_2
    assert successor.surface_uuid == SURFACE_2
    assert successor.workspace_uuid == WORKSPACE_2
    assert successor.launch_argv_template == predecessor.launch_argv_template
    assert successor.prompt_pattern == predecessor.prompt_pattern
    assert successor.entry_sha256 == predecessor.entry_sha256
    assert anchors.get(predecessor.anchor_id).state == "retired"
    assert anchors.active_for_cell(CELL) == successor

    rows = database.execute(
        "SELECT event_type, payload_json FROM events "
        "WHERE aggregate_type = 'channel_trust_anchor' ORDER BY sequence"
    ).fetchall()
    assert [str(row["event_type"]) for row in rows] == [
        "channel_trust_anchor.captured",
        "channel_trust_anchor.retired",
        "channel_trust_anchor.rebound",
    ]
    payload = json.loads(str(rows[2]["payload_json"]))
    assert payload["profile_alias"] == SELECTED_PROFILE
    assert payload["predecessor_profile_alias"] == PROFILE
    assert payload["selected_binding_id"] == seat.binding_id
    intent_row = database.execute(
        "SELECT intent_id FROM cmux_activation_intents WHERE binding_id = ?",
        (seat.binding_id,),
    ).fetchone()
    assert payload["selected_intent_id"] == str(intent_row["intent_id"])

    screen_text = f"...\n{DIALOG_TEXT}\n"
    gate, confirm, _ = _make_gate(
        database, events, anchors, control, screen_text=screen_text
    )
    result = _evaluate(
        gate,
        entry_path=rotated_entry,
        package_root=rotated_root,
        session_id=SESSION_2,
        workspace_uuid=WORKSPACE_2,
        surface_uuid=SURFACE_2,
        profile_alias=SELECTED_PROFILE,
        argv=_argv(SESSION_2, CONFIG_PATH_2),
        screen_text=screen_text,
    )
    assert result.confirmed is True
    assert result.anchor_id == successor.anchor_id
    assert confirm.calls == 1


def test_rebind_to_the_same_profile_needs_no_seat_binding(
    database: Database,
    anchors: ChannelTrustAnchors,
    package: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    """A same-profile restart or rotation carries the operator-trusted
    profile itself — no durable selection proof is consulted, exactly
    as before correction 531484d2 (no seat rows exist here at all)."""

    package_root, entry_path = package
    predecessor = _capture(anchors, package_root=package_root, entry_path=entry_path)
    rotated_root, rotated_entry = _write_package(tmp_path / "channel-pkg-gen2")

    successor = _rebind_rotated(
        anchors, package_root=rotated_root, entry_path=rotated_entry
    )

    assert successor.profile_alias == PROFILE == predecessor.profile_alias
    assert anchors.get(predecessor.anchor_id).state == "retired"
    rows = database.execute(
        "SELECT payload_json FROM events "
        "WHERE event_type = 'channel_trust_anchor.rebound'"
    ).fetchall()
    payload = json.loads(str(rows[0]["payload_json"]))
    assert payload["selected_binding_id"] is None
    assert payload["selected_intent_id"] is None


def _tamper_intent_profile(database: Database, binding_id: str) -> None:
    with database.transaction() as connection:
        connection.execute(
            "UPDATE cmux_activation_intents SET profile_alias = 'max-q' "
            "WHERE binding_id = ?",
            (binding_id,),
        )


@pytest.mark.parametrize(
    "seat",
    [
        pytest.param(
            lambda database: None,
            id="no-seat-binding-at-all",
        ),
        pytest.param(
            lambda database: _seat_replacement(database, via_intent=False),
            id="binding-minted-outside-the-intent-path",
        ),
        pytest.param(
            lambda database: _seat_replacement(database, activate=False),
            id="binding-still-residual",
        ),
        pytest.param(
            lambda database: _seat_replacement(database, profile_alias="max-q"),
            id="seat-selected-another-profile",
        ),
        pytest.param(
            lambda database: _seat_replacement(database, session_id=SESSION_3),
            id="seat-selected-another-session",
        ),
        pytest.param(
            lambda database: _seat_replacement(database, surface_uuid=SURFACE),
            id="seat-on-another-surface",
        ),
        pytest.param(
            lambda database: _seat_replacement(database, workspace_uuid=WORKSPACE),
            id="seat-in-another-workspace",
        ),
        pytest.param(
            lambda database: _tamper_intent_profile(
                database, _seat_replacement(database).binding_id
            ),
            id="intent-disagrees-with-its-binding",
        ),
    ],
)
def test_rebind_refuses_a_profile_the_durable_seat_did_not_select(
    database: Database,
    anchors: ChannelTrustAnchors,
    package: tuple[Path, Path],
    tmp_path: Path,
    seat,
) -> None:
    """The caller asks to carry trust to profile B, but Hermes's durable
    seat evidence does not prove B was selected for exactly this
    session/surface: refused before anything is measured or retired,
    with the predecessor still active and no row or event moved."""

    package_root, entry_path = package
    predecessor = _capture(anchors, package_root=package_root, entry_path=entry_path)
    rotated_root, rotated_entry = _write_package(tmp_path / "channel-pkg-gen2")
    seat(database)
    before = _anchor_rows(database)

    with pytest.raises(TrustRefused, match="Hermes-selected replacement binding"):
        _rebind_rotated(
            anchors,
            package_root=rotated_root,
            entry_path=rotated_entry,
            profile_alias=SELECTED_PROFILE,
        )

    assert _anchor_rows(database) == before
    assert anchors.get(predecessor.anchor_id).state == "active"
    assert anchors.active_for_cell(CELL) == predecessor
    assert _anchor_event_types(database) == ["channel_trust_anchor.captured"]


def test_rebind_succeeds_only_under_the_bounded_session_substitutions(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
    tmp_path: Path,
) -> None:
    """The one shape that carries forward: the replacement argv equals
    the trusted template except for the session-UUID slot and the
    session-scoped MCP config path (here even in a different, still
    well-formed directory). The successor persists the PREDECESSOR's
    template verbatim — so the config directory the replacement chose
    does not become the baseline — and a further rotation from that
    successor matches against the same original template. The gate
    then auto-confirms each rotated seat's live argv."""

    package_root, entry_path = package
    predecessor = _capture(anchors, package_root=package_root, entry_path=entry_path)
    rotated_root, rotated_entry = _write_package(tmp_path / "channel-pkg-gen2")
    relocated_config = f"/var/run/hermes/{SESSION_2}.mcp.json"

    successor = _rebind_rotated(
        anchors,
        package_root=rotated_root,
        entry_path=rotated_entry,
        launch_argv_template=_argv(SESSION_2, relocated_config),
    )

    assert successor.session_id == SESSION_2
    assert successor.profile_alias == PROFILE
    assert successor.launch_argv_template == predecessor.launch_argv_template
    assert relocated_config not in successor.launch_argv_template
    assert anchors.get(predecessor.anchor_id).state == "retired"

    screen_text = f"...\n{DIALOG_TEXT}\n"
    gate, confirm, _ = _make_gate(
        database, events, anchors, control, screen_text=screen_text
    )
    result = _evaluate(
        gate,
        entry_path=rotated_entry,
        package_root=rotated_root,
        session_id=SESSION_2,
        workspace_uuid=WORKSPACE_2,
        surface_uuid=SURFACE_2,
        argv=_argv(SESSION_2, relocated_config),
        screen_text=screen_text,
    )
    assert result.confirmed is True
    assert result.anchor_id == successor.anchor_id
    assert confirm.calls == 1

    # A second managed rotation from the successor: still measured
    # against the ORIGINAL trusted template, so the same bounded
    # substitutions for the third session are the only thing accepted.
    third = anchors.rebind(
        cell_id=CELL,
        profile_alias=PROFILE,
        entry_path=rotated_entry,
        package_root=rotated_root,
        channel_entry=CHANNEL_ENTRY,
        launch_argv_template=_argv(SESSION_3, CONFIG_PATH_3),
        workspace_uuid=WORKSPACE_2,
        surface_uuid=SURFACE,
        session_id=SESSION_3,
    )
    assert third.launch_argv_template == predecessor.launch_argv_template
    assert third.session_id == SESSION_3
    assert anchors.get(successor.anchor_id).state == "retired"

    with pytest.raises(TrustRefused, match="trusted launch template"):
        anchors.rebind(
            cell_id=CELL,
            profile_alias=PROFILE,
            entry_path=rotated_entry,
            package_root=rotated_root,
            channel_entry=CHANNEL_ENTRY,
            launch_argv_template=[*_argv(SESSION_2, CONFIG_PATH_2), "--verbose"],
            workspace_uuid=WORKSPACE_2,
            surface_uuid=SURFACE_2,
            session_id=SESSION_2,
        )
    assert anchors.active_for_cell(CELL) == third


# --------------------------------------------------------------------
# ChannelTrustAnchors.adopt (INFRA-198 blocker 1)
# --------------------------------------------------------------------

# The second visible lane: a NEW cell with no anchor of its own and no
# manual trust event to give it one — the live harness cell.
HARNESS_CELL = "cell-trust-harness"


@pytest.fixture
def seeded_harness_cell(database: Database) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO project_cells("
            "cell_id, project_key, state, profile_alias, session_id, "
            "lane_role, created_at, updated_at) "
            "VALUES (?, 'demo', 'active', ?, ?, 'harness', ?, ?)",
            (HARNESS_CELL, PROFILE, SESSION_2, NOW.isoformat(), NOW.isoformat()),
        )


def _adopt(
    anchors: ChannelTrustAnchors,
    *,
    package_root: Path,
    entry_path: Path,
    source_cell_id: str = CELL,
    cell_id: str = HARNESS_CELL,
    profile_alias: str = PROFILE,
    launch_argv_template: list[str] | None = None,
    session_id: str = SESSION_2,
):
    return anchors.adopt(
        source_cell_id=source_cell_id,
        cell_id=cell_id,
        profile_alias=profile_alias,
        entry_path=entry_path,
        package_root=package_root,
        channel_entry=CHANNEL_ENTRY,
        launch_argv_template=(
            _argv(SESSION_2, CONFIG_PATH_2)
            if launch_argv_template is None
            else launch_argv_template
        ),
        workspace_uuid=WORKSPACE_2,
        surface_uuid=SURFACE_2,
        session_id=session_id,
    )


def test_adopt_captures_the_new_cell_and_leaves_the_source_active(
    database: Database,
    anchors: ChannelTrustAnchors,
    package: tuple[Path, Path],
) -> None:
    """The one deliberate difference from rebind: adoption COPIES the
    proven material onto a second cell. The source cell — the one the
    operator's manual trust event actually named — keeps its anchor
    active, and the new cell gets a fully proven one of its own."""

    package_root, entry_path = package
    proven = _capture(anchors, package_root=package_root, entry_path=entry_path)

    adopted = _adopt(anchors, package_root=package_root, entry_path=entry_path)

    assert adopted.anchor_id != proven.anchor_id
    assert adopted.cell_id == HARNESS_CELL
    assert adopted.state == "active"
    assert anchors.active_for_cell(HARNESS_CELL) == adopted
    # The source cell is NOT retired — adoption copies, never rotates.
    assert anchors.get(proven.anchor_id).state == "active"
    assert anchors.active_for_cell(CELL) == proven

    # The proven prompt evidence and trusted launch baseline are carried
    # forward verbatim; only the seat identity is the new lane's.
    assert adopted.prompt_pattern == proven.prompt_pattern
    assert adopted.launch_argv_template == proven.launch_argv_template
    assert adopted.launch_argv_template == tuple(_argv())
    assert adopted.profile_alias == proven.profile_alias
    assert adopted.session_id == SESSION_2
    assert adopted.workspace_uuid == WORKSPACE_2
    assert adopted.surface_uuid == SURFACE_2
    # Content facts are the re-measured ones, equal to the proven anchor's.
    assert adopted.canonical_entry_path == str(entry_path)
    assert adopted.entry_sha256 == proven.entry_sha256
    assert adopted.dist_tree_sha256 == proven.dist_tree_sha256
    assert adopted.manifest_name == proven.manifest_name
    assert adopted.manifest_version == proven.manifest_version
    assert adopted.entry_owner_uid == proven.entry_owner_uid

    # No retirement event: exactly one capture and one adoption.
    assert _anchor_event_types(database) == [
        "channel_trust_anchor.captured",
        "channel_trust_anchor.adopted",
    ]
    payload = json.loads(
        str(
            database.execute(
                "SELECT payload_json FROM events "
                "WHERE aggregate_type = 'channel_trust_anchor' "
                "AND aggregate_id = ?",
                (adopted.anchor_id,),
            ).fetchone()["payload_json"]
        )
    )
    assert payload["cell_id"] == HARNESS_CELL
    assert payload["source_cell_id"] == CELL
    assert payload["predecessor_anchor_id"] == proven.anchor_id


def test_adopt_refuses_a_predecessor_with_prompt_evidence_pending(
    database: Database,
    anchors: ChannelTrustAnchors,
    package: tuple[Path, Path],
) -> None:
    """An anchor whose trust is not fully proven is never carried onto a
    second cell — exactly rebind's rule."""

    package_root, entry_path = package
    _capture(
        anchors, package_root=package_root, entry_path=entry_path, prompt_pattern=None
    )
    before = _anchor_rows(database)

    with pytest.raises(TrustRefused, match="not yet fully proven"):
        _adopt(anchors, package_root=package_root, entry_path=entry_path)

    assert _anchor_rows(database) == before
    assert anchors.active_for_cell(HARNESS_CELL) is None
    assert _anchor_event_types(database) == ["channel_trust_anchor.captured"]


def test_adopt_refuses_a_re_measured_content_drift_with_zero_mutation(
    database: Database,
    anchors: ChannelTrustAnchors,
    package: tuple[Path, Path],
) -> None:
    """Every fact is re-measured from disk at adoption time. A build
    rewritten under the trusted path is a new trust decision, not
    something the operator's one manual event proved."""

    package_root, entry_path = package
    proven = _capture(anchors, package_root=package_root, entry_path=entry_path)
    entry_path.write_text("console.log('different!');\n", encoding="utf-8")
    before = _anchor_rows(database)

    with pytest.raises(TrustRefused, match="new trust decision"):
        _adopt(anchors, package_root=package_root, entry_path=entry_path)

    assert _anchor_rows(database) == before
    assert anchors.active_for_cell(HARNESS_CELL) is None
    assert anchors.get(proven.anchor_id).state == "active"
    assert _anchor_event_types(database) == ["channel_trust_anchor.captured"]


@pytest.mark.parametrize(
    "drifted_argv",
    [
        pytest.param(
            _drift_argv(lambda a: [*a, "--verbose"]), id="extra-flag-appended"
        ),
        pytest.param(
            _drift_argv(
                lambda a: [t for t in a if t != "--dangerously-skip-permissions"]
            ),
            id="fixed-flag-dropped",
        ),
        pytest.param(
            _argv(SESSION_3, CONFIG_PATH_2), id="session-uuid-slot-is-another-session"
        ),
        pytest.param(
            _argv(SESSION_2, CONFIG_PATH_3), id="config-path-is-another-sessions"
        ),
        pytest.param([], id="empty-argv"),
    ],
)
def test_adopt_refuses_launch_argument_drift_with_zero_mutation(
    database: Database,
    anchors: ChannelTrustAnchors,
    package: tuple[Path, Path],
    drifted_argv: list[str],
) -> None:
    """The adopting lane's argv may differ from the trusted template
    only at the two bounded session slots; anything else would make
    drift the new trusted baseline."""

    package_root, entry_path = package
    proven = _capture(anchors, package_root=package_root, entry_path=entry_path)
    before = _anchor_rows(database)

    with pytest.raises(TrustRefused, match="trusted launch template"):
        _adopt(
            anchors,
            package_root=package_root,
            entry_path=entry_path,
            launch_argv_template=drifted_argv,
        )

    assert _anchor_rows(database) == before
    assert anchors.active_for_cell(HARNESS_CELL) is None
    assert anchors.active_for_cell(CELL) == proven
    assert _anchor_event_types(database) == ["channel_trust_anchor.captured"]


def test_adopt_refuses_when_the_target_cell_already_has_an_active_anchor(
    database: Database,
    anchors: ChannelTrustAnchors,
    package: tuple[Path, Path],
) -> None:
    """At most one active anchor per cell still holds: adoption only
    ever fills a cell that has none, never silently replaces a trust
    decision the target cell already carries."""

    package_root, entry_path = package
    _capture(anchors, package_root=package_root, entry_path=entry_path)
    existing = _capture(
        anchors,
        package_root=package_root,
        entry_path=entry_path,
        cell_id=HARNESS_CELL,
    )
    before = _anchor_rows(database)

    with pytest.raises(TrustRefused, match="already has an active channel trust"):
        _adopt(anchors, package_root=package_root, entry_path=entry_path)

    assert _anchor_rows(database) == before
    assert anchors.active_for_cell(HARNESS_CELL) == existing
    assert _anchor_event_types(database) == [
        "channel_trust_anchor.captured",
        "channel_trust_anchor.captured",
    ]


def test_gate_still_refuses_anchor_present_for_a_cell_with_no_anchor(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
    seeded_harness_cell: None,
) -> None:
    """No implicit project-wide fallback was introduced: a fully proven
    active anchor on a SIBLING cell of the same project does not make
    the anchorless cell pass. Adoption is an explicit step; a read path
    never mints trust as a side effect."""

    package_root, entry_path = package
    _capture(anchors, package_root=package_root, entry_path=entry_path)
    gate, confirm, read_screen = _make_gate(database, events, anchors, control)

    result = _evaluate(
        gate,
        entry_path=entry_path,
        package_root=package_root,
        cell_id=HARNESS_CELL,
        session_id=SESSION_2,
        workspace_uuid=WORKSPACE_2,
        surface_uuid=SURFACE_2,
        argv=_argv(SESSION_2, CONFIG_PATH_2),
    )

    assert result.anchor_id is None
    _assert_refused(result, control, confirm, first_failure="anchor_present")
    assert read_screen.calls == 0


def test_gate_auto_confirms_a_rebound_anchor_for_the_new_identity(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
    tmp_path: Path,
) -> None:
    """End-to-end mirror of the existing full-match gate tests: after a
    rebind, the gate auto-confirms against the successor at the new
    seat identity — the rebind is not merely a database row, it is the
    thing that lets the fail-closed gate below it keep working."""

    package_root, entry_path = package
    _capture(anchors, package_root=package_root, entry_path=entry_path)
    rotated_root, rotated_entry = _write_package(tmp_path / "channel-pkg-gen2")

    successor = anchors.rebind(
        cell_id=CELL,
        profile_alias=PROFILE,
        entry_path=rotated_entry,
        package_root=rotated_root,
        channel_entry=CHANNEL_ENTRY,
        launch_argv_template=_argv(SESSION_2, CONFIG_PATH_2),
        workspace_uuid=WORKSPACE_2,
        surface_uuid=SURFACE_2,
        session_id=SESSION_2,
    )

    screen_text = f"...\n{DIALOG_TEXT}\n"
    gate, confirm, _ = _make_gate(
        database, events, anchors, control, screen_text=screen_text
    )
    result = _evaluate(
        gate,
        entry_path=rotated_entry,
        package_root=rotated_root,
        session_id=SESSION_2,
        workspace_uuid=WORKSPACE_2,
        surface_uuid=SURFACE_2,
        argv=_argv(SESSION_2, CONFIG_PATH_2),
        screen_text=screen_text,
    )

    assert result.confirmed is True
    assert result.anchor_id == successor.anchor_id
    assert confirm.calls == 1


# --------------------------------------------------------------------
# ChannelTrustGate.evaluate — full match
# --------------------------------------------------------------------


def test_gate_full_match_confirms_once_and_records_receipt(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    package_root, entry_path = package
    anchor = _capture(anchors, package_root=package_root, entry_path=entry_path)
    screen_text = f"...\n{DIALOG_TEXT}\n"
    gate, confirm, read_screen = _make_gate(
        database, events, anchors, control, screen_text=screen_text
    )

    result = _evaluate(
        gate, entry_path=entry_path, package_root=package_root, screen_text=screen_text
    )

    assert result.confirmed is True
    assert result.anchor_id == anchor.anchor_id
    assert confirm.calls == 1
    # The one live read is the last-moment verify at the keypress
    # boundary (Sol correction a9cc6d5f packet 3); the supplied
    # screen_text served only the initial evaluation.
    assert read_screen.calls == 1
    assert result.receipt_operation_id is not None
    operation = control.get(result.receipt_operation_id)
    assert operation.kind == "channel.auto_confirmed"
    assert operation.result["anchor_id"] == anchor.anchor_id

    # The durable claim is recorded BEFORE the completion receipt and
    # binds the exact anchor + workspace + surface + launch session.
    rows = database.execute(
        "SELECT kind, dedup_key FROM control_operations ORDER BY rowid ASC"
    ).fetchall()
    assert [str(row["kind"]) for row in rows] == [
        "channel.confirm_claimed",
        "channel.auto_confirmed",
    ]
    assert str(rows[0]["dedup_key"]) == (
        f"channel.confirm:{anchor.anchor_id}:{WORKSPACE}:{SURFACE}:{SESSION}"
    )


def test_gate_reads_the_screen_when_no_text_is_supplied(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    package_root, entry_path = package
    _capture(anchors, package_root=package_root, entry_path=entry_path)
    screen_text = f"...\n{DIALOG_TEXT}\n"
    gate, confirm, read_screen = _make_gate(
        database, events, anchors, control, screen_text=screen_text
    )

    result = _evaluate(gate, entry_path=entry_path, package_root=package_root)

    assert result.confirmed is True
    # One read for the initial evaluation, one for the last-moment
    # verify immediately before the Enter (Sol correction a9cc6d5f
    # packet 3).
    assert read_screen.calls == 2
    assert confirm.calls == 1


# --------------------------------------------------------------------
# ChannelTrustGate.evaluate — fail-closed mismatches
# --------------------------------------------------------------------


def _assert_refused(
    result, control: ControlOperations, confirm: _Confirm, *, first_failure: str
) -> None:
    assert result.confirmed is False
    assert result.first_failure == first_failure
    assert confirm.calls == 0
    assert result.receipt_operation_id is not None
    operation = control.get(result.receipt_operation_id)
    assert operation.kind == "channel.approval_required"
    assert operation.reason is not None
    assert operation.reason.startswith("CHANNEL CONFIRMATION REQUIRED")
    assert operation.result["first_failure"] == first_failure


def test_gate_missing_anchor_refuses(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    package_root, entry_path = package
    gate, confirm, _ = _make_gate(database, events, anchors, control)

    result = _evaluate(gate, entry_path=entry_path, package_root=package_root)

    assert result.anchor_id is None
    _assert_refused(result, control, confirm, first_failure="anchor_present")


def test_gate_prompt_evidence_pending_refuses_before_screen_read(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    package_root, entry_path = package
    _capture(
        anchors, package_root=package_root, entry_path=entry_path, prompt_pattern=None
    )
    gate, confirm, read_screen = _make_gate(database, events, anchors, control)

    result = _evaluate(gate, entry_path=entry_path, package_root=package_root)

    _assert_refused(result, control, confirm, first_failure="prompt_evidence_pending")
    assert read_screen.calls == 0


def test_gate_symlink_substitution_of_entry_refuses(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    tmp_path: Path,
    seeded_cell: None,
) -> None:
    package_root, entry_path = package
    _capture(anchors, package_root=package_root, entry_path=entry_path)
    real_target = tmp_path / "elsewhere.js"
    real_target.write_text("evil\n", encoding="utf-8")
    entry_path.unlink()
    entry_path.symlink_to(real_target)
    gate, confirm, _ = _make_gate(database, events, anchors, control)

    result = _evaluate(gate, entry_path=entry_path, package_root=package_root)

    _assert_refused(result, control, confirm, first_failure="canonical_path")


def test_gate_owner_uid_mismatch_refuses(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    package_root, entry_path = package
    anchor = _capture(anchors, package_root=package_root, entry_path=entry_path)
    with database.transaction() as connection:
        connection.execute(
            "UPDATE channel_trust_anchors SET entry_owner_uid = ? "
            "WHERE anchor_id = ?",
            (anchor.entry_owner_uid + 1, anchor.anchor_id),
        )
    gate, confirm, _ = _make_gate(database, events, anchors, control)

    result = _evaluate(gate, entry_path=entry_path, package_root=package_root)

    _assert_refused(result, control, confirm, first_failure="owner_uid")


def test_gate_entry_content_drift_refuses(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    package_root, entry_path = package
    _capture(anchors, package_root=package_root, entry_path=entry_path)
    entry_path.write_text("console.log('tampered');\n", encoding="utf-8")
    gate, confirm, _ = _make_gate(database, events, anchors, control)

    result = _evaluate(gate, entry_path=entry_path, package_root=package_root)

    _assert_refused(result, control, confirm, first_failure="entry_sha256")


def test_gate_dist_tree_drift_extra_file_refuses(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    package_root, entry_path = package
    _capture(anchors, package_root=package_root, entry_path=entry_path)
    (package_root / "dist" / "extra.js").write_text("// smuggled\n", encoding="utf-8")
    gate, confirm, _ = _make_gate(database, events, anchors, control)

    result = _evaluate(gate, entry_path=entry_path, package_root=package_root)

    _assert_refused(result, control, confirm, first_failure="dist_tree_sha256")


def test_gate_manifest_drift_refuses(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    package_root, entry_path = package
    _capture(anchors, package_root=package_root, entry_path=entry_path)
    (package_root / "package.json").write_text(
        json.dumps({"name": "hermes-control-channel", "version": "9.9.9"}),
        encoding="utf-8",
    )
    gate, confirm, _ = _make_gate(database, events, anchors, control)

    result = _evaluate(gate, entry_path=entry_path, package_root=package_root)

    # The manifest rewrite also changes the dist-tree digest (package.json
    # is itself packaged content), so the digest check catches it first —
    # proving the digest really does cover the whole tree, not just the
    # entry file.
    _assert_refused(result, control, confirm, first_failure="dist_tree_sha256")


def test_gate_argv_extra_flag_drift_refuses(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    package_root, entry_path = package
    _capture(anchors, package_root=package_root, entry_path=entry_path)
    gate, confirm, _ = _make_gate(database, events, anchors, control)

    drifted_argv = [*_argv(), "--verbose"]
    result = _evaluate(
        gate, entry_path=entry_path, package_root=package_root, argv=drifted_argv
    )

    _assert_refused(result, control, confirm, first_failure="argv_template_match")


def test_gate_second_channel_flag_refuses(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    package_root, entry_path = package
    _capture(anchors, package_root=package_root, entry_path=entry_path)
    gate, confirm, _ = _make_gate(database, events, anchors, control)

    drifted_argv = [
        *_argv(),
        "--channels",
        "plugin:fakechat@claude-plugins-official",
    ]
    result = _evaluate(
        gate, entry_path=entry_path, package_root=package_root, argv=drifted_argv
    )

    _assert_refused(result, control, confirm, first_failure="channel_entry_single")


def test_gate_session_uuid_token_mismatch_vs_binding_session_refuses(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    package_root, entry_path = package
    _capture(anchors, package_root=package_root, entry_path=entry_path)
    gate, confirm, _ = _make_gate(database, events, anchors, control)

    # Same shape as the template, but the session-UUID slot now carries a
    # *different* (still well-formed) session id than the binding.
    drifted_argv = _argv(session_id=OTHER_UUID, config_path=CONFIG_PATH)
    result = _evaluate(
        gate,
        entry_path=entry_path,
        package_root=package_root,
        argv=drifted_argv,
        session_id=SESSION,
    )

    _assert_refused(result, control, confirm, first_failure="argv_template_match")


def test_gate_workspace_mismatch_refuses(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    package_root, entry_path = package
    _capture(anchors, package_root=package_root, entry_path=entry_path)
    gate, confirm, _ = _make_gate(database, events, anchors, control)

    result = _evaluate(
        gate,
        entry_path=entry_path,
        package_root=package_root,
        workspace_uuid=OTHER_UUID,
    )

    _assert_refused(result, control, confirm, first_failure="workspace_uuid")


def test_gate_prompt_zero_matches_refuses(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    package_root, entry_path = package
    _capture(anchors, package_root=package_root, entry_path=entry_path)
    gate, confirm, _ = _make_gate(database, events, anchors, control)

    result = _evaluate(
        gate,
        entry_path=entry_path,
        package_root=package_root,
        screen_text="nothing interesting on this screen",
    )

    _assert_refused(result, control, confirm, first_failure="prompt_match")


def test_gate_prompt_two_matches_refuses(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    package_root, entry_path = package
    _capture(anchors, package_root=package_root, entry_path=entry_path)
    gate, confirm, _ = _make_gate(database, events, anchors, control)

    screen_text = f"{DIALOG_TEXT}\n...\n{DIALOG_TEXT}"
    result = _evaluate(
        gate, entry_path=entry_path, package_root=package_root, screen_text=screen_text
    )

    _assert_refused(result, control, confirm, first_failure="prompt_match")


def test_gate_never_raises_on_a_thoroughly_broken_measurement(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    """Exception-safety: the entry file vanishing between capture and
    evaluation (a raw ``FileNotFoundError`` from ``stat``/``read_bytes``)
    is a refusal, never a raised exception."""

    package_root, entry_path = package
    _capture(anchors, package_root=package_root, entry_path=entry_path)
    entry_path.unlink()
    gate, confirm, _ = _make_gate(database, events, anchors, control)

    result = _evaluate(gate, entry_path=entry_path, package_root=package_root)

    _assert_refused(result, control, confirm, first_failure="owner_uid")


# --------------------------------------------------------------------
# Sol correction b4b545f3 packet 4 — fixed prompt matcher and the
# at-most-once durable confirmation claim.
# --------------------------------------------------------------------

# Broad, partial, and look-alike prompt patterns: none of these is the
# fixed normalized matcher shape, so none may ever authorize Enter.
REJECTED_PROMPT_PATTERNS = (
    r".*",  # broad
    r"[\s\S]+",  # broad: exactly one greedy whole-screen match
    re.escape("Enter to confirm"),  # partial: never names the channel
    PROMPT_GAP.join(  # partial: gaps, but no channel-entry segment
        (
            re.escape("Loading development channels"),
            re.escape("Enter to confirm"),
        )
    ),
    PROMPT_PATTERN.replace(  # look-alike: unescaped metacharacter
        re.escape(CHANNEL_ENTRY), "server:hermes.control"
    ),
    r"Do you trust the files in this folder\?",  # legacy arbitrary regex
    "(unterminated[",  # legacy invalid regex: fail closed, never crash
)


@pytest.mark.parametrize("pattern", REJECTED_PROMPT_PATTERNS)
def test_capture_refuses_broad_partial_and_lookalike_prompt_patterns(
    anchors: ChannelTrustAnchors, package: tuple[Path, Path], pattern: str
) -> None:
    package_root, entry_path = package

    with pytest.raises(TrustRefused, match="fixed normalized"):
        _capture(
            anchors,
            package_root=package_root,
            entry_path=entry_path,
            prompt_pattern=pattern,
        )


def test_gate_legacy_or_lookalike_prompt_rows_fail_closed_without_enter(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    """A stored row that is not the fixed normalized matcher (the live
    legacy anchor's bound prompt included) refuses closed at evaluation
    — zero keypresses, never a crash."""

    package_root, entry_path = package
    anchor = _capture(anchors, package_root=package_root, entry_path=entry_path)
    gate, confirm, _ = _make_gate(database, events, anchors, control)
    screen_text = f"...\n{DIALOG_TEXT}\n"

    for pattern in REJECTED_PROMPT_PATTERNS:
        with database.transaction() as connection:
            connection.execute(
                "UPDATE channel_trust_anchors SET prompt_pattern = ? "
                "WHERE anchor_id = ?",
                (pattern, anchor.anchor_id),
            )
        result = _evaluate(
            gate,
            entry_path=entry_path,
            package_root=package_root,
            screen_text=screen_text,
        )
        assert result.confirmed is False
        assert result.first_failure == "prompt_matcher_fixed"
    assert confirm.calls == 0


def test_gate_interleaved_and_repeated_evaluations_press_enter_at_most_once(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    """The durable claim lands BEFORE the keypress: an evaluation that
    interleaves during the external action, and every repeat after it,
    refuses without a second Enter."""

    package_root, entry_path = package
    _capture(anchors, package_root=package_root, entry_path=entry_path)
    screen_text = f"...\n{DIALOG_TEXT}\n"
    inner_gate, inner_confirm, _ = _make_gate(database, events, anchors, control)
    presses: list[str] = []
    inner_verdicts = []

    def press_and_interleave() -> None:
        presses.append("enter")
        inner_verdicts.append(
            _evaluate(
                inner_gate,
                entry_path=entry_path,
                package_root=package_root,
                screen_text=screen_text,
            )
        )

    outer_gate = ChannelTrustGate(
        database,
        events,
        anchors,
        control,
        _ReadScreen(screen_text),
        press_and_interleave,
    )
    result = _evaluate(
        outer_gate,
        entry_path=entry_path,
        package_root=package_root,
        screen_text=screen_text,
    )

    assert result.confirmed is True
    assert presses == ["enter"]
    assert inner_confirm.calls == 0
    [inner] = inner_verdicts
    assert inner.confirmed is False
    assert inner.first_failure == "confirm_already_claimed"

    repeat = _evaluate(
        outer_gate,
        entry_path=entry_path,
        package_root=package_root,
        screen_text=screen_text,
    )
    assert repeat.confirmed is False
    assert repeat.first_failure == "confirm_already_claimed"
    assert presses == ["enter"]


class _ClaimRecordingFails(ControlOperations):
    """The durable claim store refuses; everything else records."""

    def record(self, *, kind: str, **kwargs):  # type: ignore[override]
        if kind == "channel.confirm_claimed":
            raise RuntimeError("durable claim store is down")
        return super().record(kind=kind, **kwargs)


def test_gate_claim_record_failure_presses_nothing(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    package_root, entry_path = package
    failing = _ClaimRecordingFails(database, events=events)
    _capture(anchors, package_root=package_root, entry_path=entry_path)
    gate, confirm, _ = _make_gate(database, events, anchors, failing)

    result = _evaluate(
        gate,
        entry_path=entry_path,
        package_root=package_root,
        screen_text=f"...\n{DIALOG_TEXT}\n",
    )

    assert result.confirmed is False
    assert result.first_failure == "confirm_claim_failed"
    assert confirm.calls == 0
    assert result.receipt_operation_id is not None
    operation = failing.get(result.receipt_operation_id)
    assert operation.kind == "channel.approval_required"
    assert operation.reason is not None
    assert operation.reason.startswith("CHANNEL CONFIRMATION REQUIRED")


def test_gate_live_claim_for_this_launch_refuses_without_enter(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    """The claim CAS is the durable dedup key, so a live claim recorded
    by any process refuses this evaluation before the keypress."""

    package_root, entry_path = package
    anchor = _capture(anchors, package_root=package_root, entry_path=entry_path)
    claim_key = f"channel.confirm:{anchor.anchor_id}:{WORKSPACE}:{SURFACE}:{SESSION}"
    assert (
        control.record(
            kind="channel.confirm_claimed",
            project_key="demo",
            cell_id=CELL,
            session_id=SESSION,
            result={"anchor_id": anchor.anchor_id},
            dedup_key=claim_key,
        )
        is not None
    )
    gate, confirm, _ = _make_gate(database, events, anchors, control)

    result = _evaluate(
        gate,
        entry_path=entry_path,
        package_root=package_root,
        screen_text=f"...\n{DIALOG_TEXT}\n",
    )

    assert result.confirmed is False
    assert result.first_failure == "confirm_already_claimed"
    assert confirm.calls == 0


class _ExplodingConfirm:
    """Counts the keypress attempt, then fails it."""

    def __init__(self) -> None:
        self.calls = 0

    def __call__(self) -> None:
        self.calls += 1
        raise RuntimeError("cmux keypress lost")


def test_gate_keypress_failure_after_claim_is_explicit_and_not_blindly_retried(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    package_root, entry_path = package
    _capture(anchors, package_root=package_root, entry_path=entry_path)
    screen_text = f"...\n{DIALOG_TEXT}\n"
    confirm = _ExplodingConfirm()
    gate = ChannelTrustGate(
        database, events, anchors, control, _ReadScreen(screen_text), confirm
    )

    result = _evaluate(
        gate,
        entry_path=entry_path,
        package_root=package_root,
        screen_text=screen_text,
    )

    assert result.confirmed is False
    assert result.first_failure == "confirm_outcome_ambiguous"
    assert confirm.calls == 1
    assert result.receipt_operation_id is not None
    ambiguous = control.get(result.receipt_operation_id)
    assert ambiguous.kind == "channel.confirm_ambiguous"
    assert ambiguous.result["stage"] == "keypress"
    assert ambiguous.reason is not None
    assert ambiguous.reason.startswith("CHANNEL CONFIRM AMBIGUOUS")
    claim_id = str(ambiguous.result["claim_operation_id"])
    claim = control.get(claim_id)
    assert claim.kind == "channel.confirm_claimed"
    assert claim.state == "published"

    # No blind retry: the live claim keeps refusing further keypresses.
    retry = _evaluate(
        gate,
        entry_path=entry_path,
        package_root=package_root,
        screen_text=screen_text,
    )
    assert retry.confirmed is False
    assert retry.first_failure == "confirm_already_claimed"
    assert confirm.calls == 1

    # Recoverable explicitly: acknowledging both durable states frees
    # the claim for one fresh evaluation.
    assert control.acknowledge(claim_id, session_id=SESSION)
    assert control.acknowledge(ambiguous.operation_id, session_id=SESSION)
    fresh_gate, fresh_confirm, _ = _make_gate(
        database, events, anchors, control, screen_text=screen_text
    )
    recovered = _evaluate(
        fresh_gate,
        entry_path=entry_path,
        package_root=package_root,
        screen_text=screen_text,
    )
    assert recovered.confirmed is True
    assert fresh_confirm.calls == 1


class _CompletionRecordingFails(ControlOperations):
    """The completion receipt fails after the keypress; everything
    else (the claim included) records durably."""

    def record(self, *, kind: str, **kwargs):  # type: ignore[override]
        if kind == "channel.auto_confirmed":
            raise RuntimeError("receipt store failed after the keypress")
        return super().record(kind=kind, **kwargs)


def test_gate_completion_record_failure_is_a_non_success_ambiguous_verdict(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    """Sol correction f0a5a403 packet 3: when Enter succeeded but the
    ``channel.auto_confirmed`` receipt did not record, the outcome is
    AMBIGUOUS — the verdict must NOT report success. The explicit
    durable ``channel.confirm_ambiguous`` state is still recorded."""

    package_root, entry_path = package
    failing = _CompletionRecordingFails(database, events=events)
    _capture(anchors, package_root=package_root, entry_path=entry_path)
    screen_text = f"...\n{DIALOG_TEXT}\n"
    gate, confirm, _ = _make_gate(
        database, events, anchors, failing, screen_text=screen_text
    )

    result = _evaluate(
        gate,
        entry_path=entry_path,
        package_root=package_root,
        screen_text=screen_text,
    )

    assert result.confirmed is False
    assert result.first_failure == "confirm_outcome_ambiguous"
    assert confirm.calls == 1
    assert result.receipt_operation_id is not None
    ambiguous = failing.get(result.receipt_operation_id)
    assert ambiguous.kind == "channel.confirm_ambiguous"
    assert ambiguous.result["stage"] == "completion_receipt"


def test_gate_completion_receipt_ambiguity_still_prevents_another_enter(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    """The non-success ambiguous verdict retains the durable claim, so
    a repeat evaluation after a completion-receipt failure refuses
    before the keypress — never a blind second Enter."""

    package_root, entry_path = package
    failing = _CompletionRecordingFails(database, events=events)
    _capture(anchors, package_root=package_root, entry_path=entry_path)
    screen_text = f"...\n{DIALOG_TEXT}\n"
    gate, confirm, _ = _make_gate(
        database, events, anchors, failing, screen_text=screen_text
    )

    first = _evaluate(
        gate, entry_path=entry_path, package_root=package_root, screen_text=screen_text
    )
    assert first.confirmed is False
    assert confirm.calls == 1

    retry = _evaluate(
        gate, entry_path=entry_path, package_root=package_root, screen_text=screen_text
    )
    assert retry.confirmed is False
    assert retry.first_failure == "confirm_already_claimed"
    assert confirm.calls == 1


# --------------------------------------------------------------------
# Sol correction f0a5a403 packet 3 — the approved prompt marker
# sequence is a code-owned contract: exact equality at capture,
# complete_prompt, and evaluation. A caller-chosen set of escaped
# literals — even a structurally normalized one naming CHANNEL_ENTRY —
# never authorizes Enter.
# --------------------------------------------------------------------

# Each of these is structurally a "fixed normalized matcher" under the
# prior packet's contract (escaped literals of sufficient length joined
# by the exact gap, one exactly CHANNEL_ENTRY) — but none is the
# operator-approved four-marker dialog.
LOOKALIKE_MARKER_SEQUENCES = (
    # arbitrary caller-chosen replacement markers around CHANNEL_ENTRY
    (
        "A completely different dialog",
        CHANNEL_ENTRY,
        "attacker-chosen literal line",
        "press any key to continue",
    ),
    # fewer markers than the approved sequence
    ("Loading development channels", CHANNEL_ENTRY),
    # the approved sequence plus an extra trailing marker
    (*PROMPT_MARKERS, "an extra trailing marker"),
    # the approved markers, reordered
    (PROMPT_MARKERS[1], PROMPT_MARKERS[0], *PROMPT_MARKERS[2:]),
    # one literal differs
    (*PROMPT_MARKERS[:3], "Enter to continue please"),
)


def _pattern_for(markers: tuple[str, ...]) -> str:
    return PROMPT_GAP.join(re.escape(marker) for marker in markers)


@pytest.mark.parametrize("markers", LOOKALIKE_MARKER_SEQUENCES)
def test_capture_refuses_caller_chosen_normalized_marker_sequences(
    anchors: ChannelTrustAnchors,
    package: tuple[Path, Path],
    markers: tuple[str, ...],
) -> None:
    package_root, entry_path = package

    with pytest.raises(TrustRefused, match="approved"):
        _capture(
            anchors,
            package_root=package_root,
            entry_path=entry_path,
            prompt_pattern=_pattern_for(markers),
        )


@pytest.mark.parametrize("markers", LOOKALIKE_MARKER_SEQUENCES)
def test_complete_prompt_refuses_caller_chosen_normalized_marker_sequences(
    anchors: ChannelTrustAnchors,
    package: tuple[Path, Path],
    markers: tuple[str, ...],
) -> None:
    package_root, entry_path = package
    anchor = _capture(
        anchors, package_root=package_root, entry_path=entry_path, prompt_pattern=None
    )

    with pytest.raises(TrustRefused, match="approved"):
        anchors.complete_prompt(anchor.anchor_id, _pattern_for(markers))


def test_gate_only_the_exact_approved_marker_sequence_authorizes_enter(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    """Every stored look-alike normalized marker sequence fails closed
    at evaluation with zero keypresses; only the exact approved
    four-marker sequence then confirms."""

    package_root, entry_path = package
    anchor = _capture(anchors, package_root=package_root, entry_path=entry_path)
    screen_text = f"...\n{DIALOG_TEXT}\n"
    gate, confirm, _ = _make_gate(
        database, events, anchors, control, screen_text=screen_text
    )

    for markers in LOOKALIKE_MARKER_SEQUENCES:
        with database.transaction() as connection:
            connection.execute(
                "UPDATE channel_trust_anchors SET prompt_pattern = ? "
                "WHERE anchor_id = ?",
                (_pattern_for(markers), anchor.anchor_id),
            )
        result = _evaluate(
            gate,
            entry_path=entry_path,
            package_root=package_root,
            screen_text=screen_text,
        )
        assert result.confirmed is False
        assert result.first_failure == "prompt_matcher_fixed"
    assert confirm.calls == 0

    with database.transaction() as connection:
        connection.execute(
            "UPDATE channel_trust_anchors SET prompt_pattern = ? "
            "WHERE anchor_id = ?",
            (PROMPT_PATTERN, anchor.anchor_id),
        )
    approved = _evaluate(
        gate, entry_path=entry_path, package_root=package_root, screen_text=screen_text
    )
    assert approved.confirmed is True
    assert confirm.calls == 1


# --------------------------------------------------------------------
# Sol correction a9cc6d5f packet 3 — last-moment verify-and-confirm:
# Enter may be sent only when the exact approved dialog is freshly
# present on the exact bound surface at the keypress boundary. The
# gate re-reads through the caller-bound ``read_screen`` immediately
# before the Enter, inside the post-claim path; any final-boundary
# anomaly sends zero keys, records the durable non-success refusal,
# and retains the live claim so nothing blindly retries.
# --------------------------------------------------------------------


class _ExplodingReadScreen:
    """The final-boundary re-read fails (surface vanished/replaced)."""

    def __init__(self) -> None:
        self.calls = 0

    def __call__(self) -> str:
        self.calls += 1
        raise RuntimeError("cmux read-screen lost the surface")


def _final_boundary_gate(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    read_screen,
) -> tuple[ChannelTrustGate, _Confirm]:
    confirm = _Confirm()
    gate = ChannelTrustGate(database, events, anchors, control, read_screen, confirm)
    return gate, confirm


def _assert_final_boundary_refused(
    result,
    control: ControlOperations,
    confirm: _Confirm,
    database: Database,
    *,
    first_failure: str,
) -> None:
    """Zero keys, a durable non-success approval-required receipt, and
    the retained claim recorded before it."""

    _assert_refused(result, control, confirm, first_failure=first_failure)
    rows = database.execute(
        "SELECT kind FROM control_operations ORDER BY rowid ASC"
    ).fetchall()
    assert [str(row["kind"]) for row in rows] == [
        "channel.confirm_claimed",
        "channel.approval_required",
    ]


def test_gate_pane_change_after_initial_detection_sends_zero_keys(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    """Required test (1): the pane changes after initial detection but
    before confirmation — the last-moment re-read no longer shows the
    approved dialog, so zero Enter goes out; the retained claim then
    refuses any blind retry."""

    package_root, entry_path = package
    _capture(anchors, package_root=package_root, entry_path=entry_path)
    screen_text = f"...\n{DIALOG_TEXT}\n"
    changed_pane = _ReadScreen("$ vim notes.txt\nsomething else entirely\n")
    gate, confirm = _final_boundary_gate(
        database, events, anchors, control, changed_pane
    )

    result = _evaluate(
        gate, entry_path=entry_path, package_root=package_root, screen_text=screen_text
    )

    _assert_final_boundary_refused(
        result, control, confirm, database, first_failure="final_prompt_missing"
    )
    assert changed_pane.calls == 1

    retry = _evaluate(
        gate, entry_path=entry_path, package_root=package_root, screen_text=screen_text
    )
    assert retry.confirmed is False
    assert retry.first_failure == "confirm_already_claimed"
    assert confirm.calls == 0


def test_gate_fresh_dialog_at_the_boundary_sends_one_enter(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    """Required test (2): the exact approved dialog is still freshly
    present at the last moment — exactly one Enter goes out and the
    durable channel.auto_confirmed receipt records."""

    package_root, entry_path = package
    _capture(anchors, package_root=package_root, entry_path=entry_path)
    screen_text = f"...\n{DIALOG_TEXT}\n"
    fresh = _ReadScreen(screen_text)
    gate, confirm = _final_boundary_gate(database, events, anchors, control, fresh)

    result = _evaluate(
        gate, entry_path=entry_path, package_root=package_root, screen_text=screen_text
    )

    assert result.confirmed is True
    assert confirm.calls == 1
    assert fresh.calls == 1
    assert result.receipt_operation_id is not None
    assert control.get(result.receipt_operation_id).kind == "channel.auto_confirmed"


def test_gate_final_read_failure_sends_zero_keys(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    """Required test (3), read failure / replaced surface: the bounded
    re-read of the exact surface fails at the final boundary — zero
    keys, a durable non-success refusal."""

    package_root, entry_path = package
    _capture(anchors, package_root=package_root, entry_path=entry_path)
    exploding = _ExplodingReadScreen()
    gate, confirm = _final_boundary_gate(database, events, anchors, control, exploding)

    result = _evaluate(
        gate,
        entry_path=entry_path,
        package_root=package_root,
        screen_text=f"...\n{DIALOG_TEXT}\n",
    )

    _assert_final_boundary_refused(
        result, control, confirm, database, first_failure="final_read_failed"
    )
    assert exploding.calls == 1


def test_gate_multiple_prompt_matches_at_the_boundary_send_zero_keys(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    """Required test (3), multiple matches: the fresh read shows the
    dialog twice, so the match is no longer unique — zero keys."""

    package_root, entry_path = package
    _capture(anchors, package_root=package_root, entry_path=entry_path)
    doubled = _ReadScreen(f"{DIALOG_TEXT}\n...\n{DIALOG_TEXT}")
    gate, confirm = _final_boundary_gate(database, events, anchors, control, doubled)

    result = _evaluate(
        gate,
        entry_path=entry_path,
        package_root=package_root,
        screen_text=f"...\n{DIALOG_TEXT}\n",
    )

    _assert_final_boundary_refused(
        result, control, confirm, database, first_failure="final_prompt_multiple"
    )


def test_gate_dialog_content_drift_at_the_boundary_sends_zero_keys(
    database: Database,
    events: EventStore,
    anchors: ChannelTrustAnchors,
    control: ControlOperations,
    package: tuple[Path, Path],
    seeded_cell: None,
) -> None:
    """Required test (3), content drift: the fresh read still matches
    the fixed matcher exactly once, but the matched dialog is not
    byte-identical to the one the claim was recorded against (here the
    development checkbox flipped) — zero keys."""

    package_root, entry_path = package
    _capture(anchors, package_root=package_root, entry_path=entry_path)
    drifted = _ReadScreen(
        f"...\n{DIALOG_TEXT.replace('[x] I am using', '[ ] I am using')}\n"
    )
    gate, confirm = _final_boundary_gate(database, events, anchors, control, drifted)

    result = _evaluate(
        gate,
        entry_path=entry_path,
        package_root=package_root,
        screen_text=f"...\n{DIALOG_TEXT}\n",
    )

    _assert_final_boundary_refused(
        result, control, confirm, database, first_failure="final_prompt_drift"
    )


# Sol correction a06cbce0: prompt-shape validation must be structural
# to the DISPLAYED dialog Channels list, not a raw substring count over
# the whole captured screen — a full-scrollback capture legitimately
# echoes the shell's own
# ``--dangerously-load-development-channels server:hermes-control``
# launch argument above the dialog, which must never be conjured into
# a second Channels entry.

_LAUNCH_ECHO = (
    "claude --resume 11111111-1111-4111-8111-111111111111 "
    "--dangerously-skip-permissions "
    "--dangerously-load-development-channels server:hermes-control\n"
    "\n"
)


def test_dialog_region_excludes_text_above_the_opening_marker() -> None:
    screen = _LAUNCH_ECHO + DIALOG_TEXT + "\ntrailing noise"

    region = _dialog_region(screen)

    assert region is not None
    assert region.startswith("Loading development channels")
    assert "--dangerously-load-development-channels" not in region
    assert region.endswith("Enter to confirm")
    assert "trailing noise" not in region


def test_dialog_region_is_none_without_both_boundary_markers() -> None:
    assert _dialog_region(_LAUNCH_ECHO) is None
    assert _dialog_region("Loading development channels\nno end here") is None
    # The closing marker appearing only BEFORE the opening one does not
    # count as bounding a region.
    assert _dialog_region("Enter to confirm\nLoading development channels") is None


def test_displayed_channel_entries_valid_ignores_echoed_launch_argv() -> None:
    """The regression: `server:hermes-control` legitimately appears
    twice in the full screen (once in the echoed shell argv, once in
    the displayed Channels line) but only once inside the dialog
    region — this must validate."""

    screen = _LAUNCH_ECHO + DIALOG_TEXT

    assert screen.count(CHANNEL_ENTRY) == 2
    assert displayed_channel_entries_valid(screen) is True


def test_displayed_channel_entries_valid_fails_closed_on_second_displayed_entry() -> (
    None
):
    screen = _LAUNCH_ECHO + DIALOG_TEXT.replace(
        f"  - {CHANNEL_ENTRY}\n",
        f"  - {CHANNEL_ENTRY}\n  - server:evil\n",
    )

    assert displayed_channel_entries_valid(screen) is False


def test_displayed_channel_entries_valid_fails_closed_on_duplicate_entry() -> None:
    """A second DISPLAYED ``hermes-control`` line inside the dialog
    region — not merely echoed argv above it — still refuses."""

    screen = _LAUNCH_ECHO + DIALOG_TEXT.replace(
        f"  - {CHANNEL_ENTRY}\n",
        f"  - {CHANNEL_ENTRY}\n  - {CHANNEL_ENTRY}\n",
    )

    assert displayed_channel_entries_valid(screen) is False


def test_displayed_channel_entries_valid_fails_closed_without_a_dialog_region() -> None:
    assert displayed_channel_entries_valid(_LAUNCH_ECHO) is False
    assert displayed_channel_entries_valid("") is False
