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
)
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


@pytest.fixture
def package(tmp_path: Path) -> tuple[Path, Path]:
    package_root = tmp_path / "channel-pkg"
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
    gate, confirm, _ = _make_gate(database, events, anchors, control)
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
    gate, confirm, _ = _make_gate(database, events, anchors, control)

    result = _evaluate(
        gate, entry_path=entry_path, package_root=package_root, screen_text=screen_text
    )

    assert result.confirmed is True
    assert result.anchor_id == anchor.anchor_id
    assert confirm.calls == 1
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
    assert read_screen.calls == 1
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
    assert operation.reason.startswith("CHANNEL APPROVAL REQUIRED")
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
        database, events, anchors, control, _ReadScreen(), press_and_interleave
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
    assert operation.reason.startswith("CHANNEL APPROVAL REQUIRED")


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
        database, events, anchors, control, _ReadScreen(), confirm
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
    fresh_gate, fresh_confirm, _ = _make_gate(database, events, anchors, control)
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
    gate, confirm, _ = _make_gate(database, events, anchors, failing)

    result = _evaluate(
        gate,
        entry_path=entry_path,
        package_root=package_root,
        screen_text=f"...\n{DIALOG_TEXT}\n",
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
    gate, confirm, _ = _make_gate(database, events, anchors, failing)

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
    gate, confirm, _ = _make_gate(database, events, anchors, control)
    screen_text = f"...\n{DIALOG_TEXT}\n"

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
