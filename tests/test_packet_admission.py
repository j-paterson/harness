"""Packet admission service tests (INFRA-186)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.model_tiers import ModelTier, load_model_tiers
from hermes_orchestrator.packet_admission import AdmissionDecision, PacketAdmission
from hermes_orchestrator.subagent_packets import SubagentPacket, SubagentPackets

NOW = datetime(2026, 8, 30, 12, tzinfo=UTC)
SESSION = "11111111-2222-4333-8444-555555555555"
FOREIGN_SESSION = "66666666-7777-4888-9999-aaaaaaaaaaaa"
CELL_ID = "cell-demo"
PROJECT_KEY = "demo"
TOOL_USE_A = "tool-use-a"
TOOL_USE_B = "tool-use-b"

CONFIG_PATH = Path(__file__).parent.parent / "config" / "model-tiers.yaml"


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def packets(database: Database) -> SubagentPackets:
    return SubagentPackets(database, events=EventStore(database), now=lambda: NOW)


@pytest.fixture
def tiers() -> dict[str, ModelTier]:
    return load_model_tiers(CONFIG_PATH)


@pytest.fixture
def freeze_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "freeze"
    directory.mkdir()
    return directory


@pytest.fixture
def admission(
    database: Database,
    packets: SubagentPackets,
    tiers: dict[str, ModelTier],
    freeze_dir: Path,
) -> PacketAdmission:
    return PacketAdmission(
        database,
        packets=packets,
        tiers=tiers,
        freeze_dir=freeze_dir,
        now=lambda: NOW,
    )


def seed_active_cell(
    database: Database, *, session_id: str = SESSION, cell_id: str = CELL_ID
) -> None:
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO project_cells("
            "cell_id, project_key, state, profile_alias, session_id, "
            "created_at, updated_at) VALUES (?, ?, 'active', 'max-c', ?, ?, ?)",
            (cell_id, PROJECT_KEY, session_id, NOW.isoformat(), NOW.isoformat()),
        )


def create_packet(
    packets: SubagentPackets,
    *,
    issue_id: str = "INFRA-186",
    session_id: str = SESSION,
    cell_id: str = CELL_ID,
    allowed_files: tuple[str, ...] = ("src/hermes_orchestrator/foo.py",),
    worktree: str = "/work/demo",
    model_tier: str = "sonnet",
    effort: str = "high",
    depends_on: tuple[str, ...] = (),
) -> SubagentPacket:
    return packets.create(
        issue_id=issue_id,
        project_key=PROJECT_KEY,
        cell_id=cell_id,
        session_id=session_id,
        generation=1,
        model_tier=model_tier,
        effort=effort,
        allowed_files=allowed_files,
        worktree=worktree,
        depends_on=depends_on,
        red_test="tests/test_foo.py::test_red",
        verification=("uv run pytest tests/test_foo.py -q",),
        invariants="never touch bar.py",
        resource_note="single worker",
    )


def test_admit_refuses_unknown_packet(
    database: Database, admission: PacketAdmission
) -> None:
    seed_active_cell(database)

    decision = admission.admit(
        session_id=SESSION,
        packet_id="does-not-exist",
        model="sonnet",
        effort="high",
        tool_use_id=TOOL_USE_A,
    )

    assert decision == AdmissionDecision(
        allowed=False, reason="unknown packet", packet_id=None
    )


def test_admit_refuses_a_foreign_or_inactive_session(
    database: Database, packets: SubagentPackets, admission: PacketAdmission
) -> None:
    seed_active_cell(database, session_id=FOREIGN_SESSION, cell_id="cell-other")
    packet = create_packet(packets)

    decision = admission.admit(
        session_id=SESSION,
        packet_id=packet.packet_id,
        model="sonnet",
        effort="high",
        tool_use_id=TOOL_USE_A,
    )

    assert decision.allowed is False
    assert decision.reason == "no active cell owns this session"


def test_admit_refuses_wrong_model_tier(
    database: Database, packets: SubagentPackets, admission: PacketAdmission
) -> None:
    seed_active_cell(database)
    packet = create_packet(packets, model_tier="sonnet")

    decision = admission.admit(
        session_id=SESSION,
        packet_id=packet.packet_id,
        model="haiku",
        effort="high",
        tool_use_id=TOOL_USE_A,
    )

    assert decision.allowed is False
    assert decision.reason == "packet requires tier sonnet"


def test_admit_refuses_effort_outside_the_enum(
    database: Database, packets: SubagentPackets, admission: PacketAdmission
) -> None:
    seed_active_cell(database)
    packet = create_packet(packets, model_tier="sonnet", effort="high")

    decision = admission.admit(
        session_id=SESSION,
        packet_id=packet.packet_id,
        model="sonnet",
        effort="extreme",
        tool_use_id=TOOL_USE_A,
    )

    assert decision.allowed is False
    assert decision.reason == "packet requires tier sonnet"


def test_admit_refuses_overlapping_allowed_files_with_a_reserved_packet(
    database: Database, packets: SubagentPackets, admission: PacketAdmission
) -> None:
    seed_active_cell(database)
    reserved = create_packet(
        packets,
        issue_id="INFRA-186-a",
        allowed_files=("src/hermes_orchestrator/shared.py",),
        worktree="/work/a",
    )
    packets.reserve(reserved.packet_id, session_id=SESSION, tool_use_id=TOOL_USE_A)
    candidate = create_packet(
        packets,
        issue_id="INFRA-186-b",
        allowed_files=("src/hermes_orchestrator/shared.py",),
        worktree="/work/b",
    )

    decision = admission.admit(
        session_id=SESSION,
        packet_id=candidate.packet_id,
        model="sonnet",
        effort="high",
        tool_use_id=TOOL_USE_B,
    )

    assert decision.allowed is False
    assert decision.reason == (
        f"overlapping ownership with reserved packet {reserved.packet_id}"
    )


def test_admit_allows_disjoint_allowed_files(
    database: Database, packets: SubagentPackets, admission: PacketAdmission
) -> None:
    seed_active_cell(database)
    reserved = create_packet(
        packets,
        issue_id="INFRA-186-a",
        allowed_files=("src/hermes_orchestrator/shared.py",),
        worktree="/work/a",
    )
    packets.reserve(reserved.packet_id, session_id=SESSION, tool_use_id=TOOL_USE_A)
    candidate = create_packet(
        packets,
        issue_id="INFRA-186-b",
        allowed_files=("src/hermes_orchestrator/other.py",),
        worktree="/work/b",
    )

    decision = admission.admit(
        session_id=SESSION,
        packet_id=candidate.packet_id,
        model="sonnet",
        effort="high",
        tool_use_id=TOOL_USE_B,
    )

    assert decision.allowed is True
    assert decision.packet_id == candidate.packet_id


def test_admit_refuses_the_same_worktree_as_a_reserved_packet(
    database: Database, packets: SubagentPackets, admission: PacketAdmission
) -> None:
    seed_active_cell(database)
    reserved = create_packet(
        packets,
        issue_id="INFRA-186-a",
        allowed_files=("src/hermes_orchestrator/a.py",),
        worktree="/work/shared",
    )
    packets.reserve(reserved.packet_id, session_id=SESSION, tool_use_id=TOOL_USE_A)
    candidate = create_packet(
        packets,
        issue_id="INFRA-186-b",
        allowed_files=("src/hermes_orchestrator/b.py",),
        worktree="/work/shared",
    )

    decision = admission.admit(
        session_id=SESSION,
        packet_id=candidate.packet_id,
        model="sonnet",
        effort="high",
        tool_use_id=TOOL_USE_B,
    )

    assert decision.allowed is False
    assert decision.reason == (
        f"overlapping ownership with reserved packet {reserved.packet_id}"
    )


def test_admit_refuses_under_resource_pressure(
    database: Database, packets: SubagentPackets, tiers: dict[str, ModelTier],
    freeze_dir: Path,
) -> None:
    seed_active_cell(database)
    packet = create_packet(packets)
    admission = PacketAdmission(
        database,
        packets=packets,
        tiers=tiers,
        freeze_dir=freeze_dir,
        headroom=lambda: False,
        now=lambda: NOW,
    )

    decision = admission.admit(
        session_id=SESSION,
        packet_id=packet.packet_id,
        model="sonnet",
        effort="high",
        tool_use_id=TOOL_USE_A,
    )

    assert decision.allowed is False
    assert decision.reason == "resource pressure refuses new subagents"


def test_admit_refuses_with_the_freeze_markers_reason(
    database: Database,
    packets: SubagentPackets,
    admission: PacketAdmission,
    freeze_dir: Path,
) -> None:
    seed_active_cell(database)
    packet = create_packet(packets)
    marker = freeze_dir / f"{SESSION}.frozen"
    marker.write_text("rotation in progress", encoding="utf-8")

    decision = admission.admit(
        session_id=SESSION,
        packet_id=packet.packet_id,
        model="sonnet",
        effort="high",
        tool_use_id=TOOL_USE_A,
    )

    assert decision.allowed is False
    assert decision.reason == "rotation in progress"


def test_admit_refuses_incomplete_dependencies_via_the_ledger(
    database: Database, packets: SubagentPackets, admission: PacketAdmission
) -> None:
    seed_active_cell(database)
    dependency = create_packet(packets, issue_id="INFRA-186-dep")
    dependent = create_packet(
        packets, issue_id="INFRA-186", depends_on=(dependency.packet_id,)
    )

    decision = admission.admit(
        session_id=SESSION,
        packet_id=dependent.packet_id,
        model="sonnet",
        effort="high",
        tool_use_id=TOOL_USE_A,
    )

    assert decision.allowed is False
    assert "not accepted" in decision.reason


def test_admit_reserves_the_packet_on_success(
    database: Database, packets: SubagentPackets, admission: PacketAdmission
) -> None:
    seed_active_cell(database)
    packet = create_packet(packets)

    decision = admission.admit(
        session_id=SESSION,
        packet_id=packet.packet_id,
        model="sonnet",
        effort="high",
        tool_use_id=TOOL_USE_A,
    )

    assert decision == AdmissionDecision(
        allowed=True, reason="reserved", packet_id=packet.packet_id
    )
    reserved = packets.get(packet.packet_id)
    assert reserved.state == "reserved"
    row = database.execute(
        "SELECT tool_use_id FROM subagent_packets WHERE packet_id = ?",
        (packet.packet_id,),
    ).fetchone()
    assert str(row["tool_use_id"]) == TOOL_USE_A


def test_admit_is_exactly_once_on_double_admission(
    database: Database, packets: SubagentPackets, admission: PacketAdmission
) -> None:
    seed_active_cell(database)
    packet = create_packet(packets)

    first = admission.admit(
        session_id=SESSION,
        packet_id=packet.packet_id,
        model="sonnet",
        effort="high",
        tool_use_id=TOOL_USE_A,
    )
    second = admission.admit(
        session_id=SESSION,
        packet_id=packet.packet_id,
        model="sonnet",
        effort="high",
        tool_use_id=TOOL_USE_B,
    )

    assert first.allowed is True
    assert second.allowed is False
    assert packets.get(packet.packet_id).state == "reserved"
