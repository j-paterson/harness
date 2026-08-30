"""The bounded dev-channel trust-anchor state machine (INFRA-197 v5.1).

Operator decision ``infra-197-trusted-channel-auto-approval-20260830-v1``:
after ONE exact manual trust event of ONE exact hermes-control
development-channel build, a narrowly scoped automatic confirmation may
stand in for Claude Code's per-launch development-channel dialog.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.channel_trust import (
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

PROMPT_SUBSTRING = "Do you trust the files in this folder?"
PROMPT_PATTERN = PROMPT_SUBSTRING.replace("?", r"\?")


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

    screen_text = f"...\n{PROMPT_SUBSTRING}\n(y/n)"
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


def test_complete_prompt_refuses_invalid_regex(
    anchors: ChannelTrustAnchors, package: tuple[Path, Path]
) -> None:
    package_root, entry_path = package
    anchor = _capture(
        anchors, package_root=package_root, entry_path=entry_path, prompt_pattern=None
    )

    with pytest.raises(TrustRefused, match="not a valid regular expression"):
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
    screen_text = f"...\n{PROMPT_SUBSTRING}\n(y/n)"
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
    screen_text = f"...\n{PROMPT_SUBSTRING}\n(y/n)"
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

    screen_text = f"{PROMPT_SUBSTRING}\n...\n{PROMPT_SUBSTRING}"
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
