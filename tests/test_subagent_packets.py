"""Durable subagent-packet ledger tests (INFRA-186)."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.events import EventStore
from hermes_orchestrator.subagent_packets import (
    PacketRefused,
    SubagentPacket,
    SubagentPackets,
)

NOW = datetime(2026, 8, 30, 12, tzinfo=UTC)
SESSION_A = "11111111-2222-4333-8444-555555555555"
SESSION_B = "66666666-7777-4888-9999-aaaaaaaaaaaa"
TOOL_USE_A = "tool-use-a"
TOOL_USE_B = "tool-use-b"


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


def create(
    packets: SubagentPackets,
    *,
    issue_id: str = "INFRA-186",
    session_id: str = SESSION_A,
    depends_on: tuple[str, ...] = (),
    allowed_files: tuple[str, ...] = ("src/hermes_orchestrator/foo.py",),
    model_tier: str = "sonnet",
) -> SubagentPacket:
    return packets.create(
        issue_id=issue_id,
        project_key="demo",
        cell_id="cell-demo",
        session_id=session_id,
        generation=1,
        model_tier=model_tier,
        effort="medium",
        allowed_files=allowed_files,
        worktree="/work/demo",
        depends_on=depends_on,
        red_test="tests/test_foo.py::test_red",
        verification=("uv run pytest tests/test_foo.py -q",),
        invariants="never touch bar.py",
        resource_note="single worker",
    )


def events_of(database: Database, event_type: str) -> list[str]:
    rows = database.execute(
        "SELECT aggregate_id FROM events WHERE event_type = ?",
        (event_type,),
    ).fetchall()
    return [str(row["aggregate_id"]) for row in rows]


def test_create_returns_a_planned_packet_and_journals_it(
    packets: SubagentPackets, database: Database
) -> None:
    packet = create(packets)

    assert packet.state == "planned"
    assert packet.issue_id == "INFRA-186"
    assert packet.model_tier == "sonnet"
    assert packet.allowed_files == ("src/hermes_orchestrator/foo.py",)
    assert packet.depends_on == ()
    assert packet.evidence is None
    assert packets.get(packet.packet_id) == packet
    assert events_of(database, "subagent_packet.planned") == [packet.packet_id]


def test_create_refuses_unknown_model_tier(packets: SubagentPackets) -> None:
    with pytest.raises(PacketRefused):
        create(packets, model_tier="opus")


def test_create_refuses_empty_allowed_files(packets: SubagentPackets) -> None:
    with pytest.raises(PacketRefused):
        create(packets, allowed_files=())


def test_create_refuses_missing_dependency(packets: SubagentPackets) -> None:
    with pytest.raises(PacketRefused):
        create(packets, depends_on=("does-not-exist",))


def test_get_refuses_unknown_packet(packets: SubagentPackets) -> None:
    with pytest.raises(KeyError):
        packets.get("does-not-exist")


def test_reserve_refuses_unknown_packet(packets: SubagentPackets) -> None:
    with pytest.raises(PacketRefused):
        packets.reserve(
            "does-not-exist", session_id=SESSION_A, tool_use_id=TOOL_USE_A
        )


def test_reserve_refuses_a_foreign_session(packets: SubagentPackets) -> None:
    packet = create(packets, session_id=SESSION_A)

    with pytest.raises(PacketRefused):
        packets.reserve(
            packet.packet_id, session_id=SESSION_B, tool_use_id=TOOL_USE_A
        )


def test_reserve_is_exactly_once(
    packets: SubagentPackets, database: Database
) -> None:
    packet = create(packets)

    reserved = packets.reserve(
        packet.packet_id, session_id=SESSION_A, tool_use_id=TOOL_USE_A
    )
    assert reserved.state == "reserved"

    with pytest.raises(PacketRefused):
        packets.reserve(
            packet.packet_id, session_id=SESSION_A, tool_use_id=TOOL_USE_B
        )

    assert events_of(database, "subagent_packet.reserved") == [packet.packet_id]


def test_reserve_refuses_incomplete_dependencies_then_succeeds(
    packets: SubagentPackets,
) -> None:
    dependency = create(packets, issue_id="INFRA-186-dep")
    dependent = create(
        packets, issue_id="INFRA-186", depends_on=(dependency.packet_id,)
    )

    with pytest.raises(PacketRefused):
        packets.reserve(
            dependent.packet_id, session_id=SESSION_A, tool_use_id=TOOL_USE_A
        )

    reserved_dep = packets.reserve(
        dependency.packet_id, session_id=SESSION_A, tool_use_id=TOOL_USE_A
    )
    packets.settle(
        reserved_dep.packet_id, outcome="completed", tool_use_id=TOOL_USE_A
    )
    packets.accept(reserved_dep.packet_id, evidence={"diff_scope": ["foo.py"]})

    reserved_dependent = packets.reserve(
        dependent.packet_id, session_id=SESSION_A, tool_use_id=TOOL_USE_B
    )
    assert reserved_dependent.state == "reserved"


def test_settle_maps_outcomes_and_requires_the_reserving_tool_use_id(
    packets: SubagentPackets, database: Database
) -> None:
    completed = create(packets, issue_id="INFRA-186-a")
    failed = create(packets, issue_id="INFRA-186-b")
    capped = create(packets, issue_id="INFRA-186-c")
    for packet in (completed, failed, capped):
        packets.reserve(
            packet.packet_id, session_id=SESSION_A, tool_use_id=TOOL_USE_A
        )

    settled_completed = packets.settle(
        completed.packet_id, outcome="completed", tool_use_id=TOOL_USE_A
    )
    settled_failed = packets.settle(
        failed.packet_id, outcome="failed", tool_use_id=TOOL_USE_A
    )
    settled_capped = packets.settle(
        capped.packet_id, outcome="capped", tool_use_id=TOOL_USE_A
    )

    assert settled_completed.state == "returned"
    assert settled_failed.state == "failed"
    assert settled_capped.state == "failed"
    assert events_of(database, "subagent_packet.returned") == [
        completed.packet_id
    ]


def test_settle_refuses_a_mismatched_tool_use_id(
    packets: SubagentPackets,
) -> None:
    packet = create(packets)
    packets.reserve(
        packet.packet_id, session_id=SESSION_A, tool_use_id=TOOL_USE_A
    )

    with pytest.raises(PacketRefused):
        packets.settle(
            packet.packet_id, outcome="completed", tool_use_id=TOOL_USE_B
        )
    assert packets.get(packet.packet_id).state == "reserved"


def test_settle_is_exactly_once(packets: SubagentPackets) -> None:
    packet = create(packets)
    packets.reserve(
        packet.packet_id, session_id=SESSION_A, tool_use_id=TOOL_USE_A
    )
    packets.settle(packet.packet_id, outcome="completed", tool_use_id=TOOL_USE_A)

    with pytest.raises(PacketRefused):
        packets.settle(
            packet.packet_id, outcome="completed", tool_use_id=TOOL_USE_A
        )


def test_accept_stores_evidence_and_is_terminal(
    packets: SubagentPackets, database: Database
) -> None:
    packet = create(packets)
    packets.reserve(
        packet.packet_id, session_id=SESSION_A, tool_use_id=TOOL_USE_A
    )
    packets.settle(packet.packet_id, outcome="completed", tool_use_id=TOOL_USE_A)

    evidence = {"diff_scope": ["foo.py"], "red_green": "tests/test_foo.py"}
    accepted = packets.accept(packet.packet_id, evidence=evidence)

    assert accepted.state == "accepted"
    assert accepted.evidence == evidence
    assert packets.get(packet.packet_id).evidence == evidence
    assert events_of(database, "subagent_packet.accepted") == [packet.packet_id]

    with pytest.raises(PacketRefused):
        packets.accept(packet.packet_id, evidence=evidence)
    with pytest.raises(PacketRefused):
        packets.reject(packet.packet_id, reason="too late")


def test_accept_never_journals_evidence_values(
    packets: SubagentPackets, database: Database
) -> None:
    packet = create(packets)
    packets.reserve(
        packet.packet_id, session_id=SESSION_A, tool_use_id=TOOL_USE_A
    )
    packets.settle(packet.packet_id, outcome="completed", tool_use_id=TOOL_USE_A)
    packets.accept(
        packet.packet_id,
        evidence={"prompt": "do not leak this model output text"},
    )

    row = database.execute(
        "SELECT payload_json FROM events "
        "WHERE event_type = 'subagent_packet.accepted' AND aggregate_id = ?",
        (packet.packet_id,),
    ).fetchone()
    assert "do not leak this model output text" not in str(row["payload_json"])


def test_reject_from_returned(packets: SubagentPackets, database: Database) -> None:
    packet = create(packets)
    packets.reserve(
        packet.packet_id, session_id=SESSION_A, tool_use_id=TOOL_USE_A
    )
    packets.settle(packet.packet_id, outcome="completed", tool_use_id=TOOL_USE_A)

    rejected = packets.reject(packet.packet_id, reason="wrong scope")

    assert rejected.state == "rejected"
    assert events_of(database, "subagent_packet.rejected") == [packet.packet_id]


def test_reject_from_reserved(packets: SubagentPackets) -> None:
    packet = create(packets)
    packets.reserve(
        packet.packet_id, session_id=SESSION_A, tool_use_id=TOOL_USE_A
    )

    rejected = packets.reject(packet.packet_id, reason="scope creep")

    assert rejected.state == "rejected"


def test_reject_refuses_from_planned(packets: SubagentPackets) -> None:
    packet = create(packets)

    with pytest.raises(PacketRefused):
        packets.reject(packet.packet_id, reason="too soon")


def test_expire_orphans_expires_a_proven_orphan_and_preserves_a_live_one(
    packets: SubagentPackets, database: Database
) -> None:
    orphan = create(packets, issue_id="INFRA-186-orphan")
    live = create(packets, issue_id="INFRA-186-live")
    packets.reserve(
        orphan.packet_id, session_id=SESSION_A, tool_use_id=TOOL_USE_A
    )
    packets.reserve(live.packet_id, session_id=SESSION_A, tool_use_id=TOOL_USE_B)

    expired = packets.expire_orphans(live_tool_use_ids=frozenset({TOOL_USE_B}))

    assert [packet.packet_id for packet in expired] == [orphan.packet_id]
    assert packets.get(orphan.packet_id).state == "failed"
    assert packets.get(live.packet_id).state == "reserved"
    assert events_of(database, "subagent_packet.failed") == [orphan.packet_id]


def test_for_issue_lists_only_that_issues_packets(
    packets: SubagentPackets,
) -> None:
    a1 = create(packets, issue_id="INFRA-186-a")
    a2 = create(packets, issue_id="INFRA-186-a")
    create(packets, issue_id="INFRA-186-b")

    found = packets.for_issue("INFRA-186-a")

    assert [packet.packet_id for packet in found] == [a1.packet_id, a2.packet_id]


# --- INFRA-194: reservation and settlement carry a measured binding to
# the work actually performed (execution provenance). ---


def test_reserve_stores_measured_blobs_including_absent_sentinel(
    database: Database,
) -> None:
    present_path = "/work/demo/present.py"
    content = b"print('hello')"

    def read_file(path: Path) -> bytes | None:
        if str(path) == present_path:
            return content
        return None

    packets = SubagentPackets(
        database, events=EventStore(database), now=lambda: NOW, read_file=read_file
    )
    packet = create(packets, allowed_files=("present.py", "missing.py"))

    reserved = packets.reserve(
        packet.packet_id, session_id=SESSION_A, tool_use_id=TOOL_USE_A
    )

    assert reserved.reserved_blobs == {
        "present.py": hashlib.sha256(content).hexdigest(),
        "missing.py": "absent",
    }
    assert packets.get(packet.packet_id).reserved_blobs == reserved.reserved_blobs


def test_reserve_refuses_when_measurement_raises_and_leaves_packet_planned(
    database: Database,
) -> None:
    def read_file(path: Path) -> bytes | None:
        raise OSError("worktree unreadable")

    packets = SubagentPackets(
        database, events=EventStore(database), now=lambda: NOW, read_file=read_file
    )
    packet = create(packets)

    with pytest.raises(PacketRefused):
        packets.reserve(
            packet.packet_id, session_id=SESSION_A, tool_use_id=TOOL_USE_A
        )

    reloaded = packets.get(packet.packet_id)
    assert reloaded.state == "planned"
    assert reloaded.reserved_blobs is None


def test_completed_settle_stores_returned_blobs_reflecting_changed_content(
    database: Database,
) -> None:
    path_str = "/work/demo/present.py"
    state = {"content": b"before"}

    def read_file(path: Path) -> bytes | None:
        if str(path) == path_str:
            return state["content"]
        return None

    packets = SubagentPackets(
        database, events=EventStore(database), now=lambda: NOW, read_file=read_file
    )
    packet = create(packets, allowed_files=("present.py",))
    reserved = packets.reserve(
        packet.packet_id, session_id=SESSION_A, tool_use_id=TOOL_USE_A
    )
    assert reserved.reserved_blobs == {
        "present.py": hashlib.sha256(b"before").hexdigest()
    }

    state["content"] = b"after"
    settled = packets.settle(
        packet.packet_id, outcome="completed", tool_use_id=TOOL_USE_A
    )

    assert settled.returned_blobs == {
        "present.py": hashlib.sha256(b"after").hexdigest()
    }
    assert settled.returned_blobs != settled.reserved_blobs


def test_completed_settle_refuses_when_measurement_raises_and_leaves_reserved(
    database: Database,
) -> None:
    calls = {"n": 0}

    def read_file(path: Path) -> bytes | None:
        calls["n"] += 1
        if calls["n"] == 1:
            return b"ok"
        raise OSError("worktree unreadable")

    packets = SubagentPackets(
        database, events=EventStore(database), now=lambda: NOW, read_file=read_file
    )
    packet = create(packets)
    packets.reserve(
        packet.packet_id, session_id=SESSION_A, tool_use_id=TOOL_USE_A
    )

    with pytest.raises(PacketRefused):
        packets.settle(
            packet.packet_id, outcome="completed", tool_use_id=TOOL_USE_A
        )

    reloaded = packets.get(packet.packet_id)
    assert reloaded.state == "reserved"
    assert reloaded.returned_blobs is None


def test_failed_settle_leaves_returned_blobs_none(
    packets: SubagentPackets,
) -> None:
    packet = create(packets)
    packets.reserve(
        packet.packet_id, session_id=SESSION_A, tool_use_id=TOOL_USE_A
    )

    settled = packets.settle(
        packet.packet_id, outcome="failed", tool_use_id=TOOL_USE_A
    )

    assert settled.state == "failed"
    assert settled.returned_blobs is None


def test_legacy_rows_without_provenance_load_as_none(
    packets: SubagentPackets, database: Database
) -> None:
    packet = create(packets)
    packets.reserve(
        packet.packet_id, session_id=SESSION_A, tool_use_id=TOOL_USE_A
    )
    packets.settle(packet.packet_id, outcome="completed", tool_use_id=TOOL_USE_A)
    packets.accept(packet.packet_id, evidence={"diff_scope": ["foo.py"]})

    with database.transaction() as connection:
        connection.execute(
            "UPDATE subagent_packets SET reserved_blobs_json = NULL, "
            "returned_blobs_json = NULL WHERE packet_id = ?",
            (packet.packet_id,),
        )

    reloaded = packets.get(packet.packet_id)
    assert reloaded.reserved_blobs is None
    assert reloaded.returned_blobs is None


def test_handoff_summary_contains_no_blob_values(
    packets: SubagentPackets,
) -> None:
    packet = create(packets)
    packets.reserve(
        packet.packet_id, session_id=SESSION_A, tool_use_id=TOOL_USE_A
    )
    packets.settle(packet.packet_id, outcome="completed", tool_use_id=TOOL_USE_A)

    summary = packets.handoff_summary(packet.issue_id)

    rendered = json.dumps(summary)
    assert "reserved_blobs" not in rendered
    assert "returned_blobs" not in rendered
