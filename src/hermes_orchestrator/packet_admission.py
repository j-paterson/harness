"""Packet admission service (INFRA-186).

The narrow gate that decides whether one Agent-tool subagent launch may
proceed: it either atomically reserves the requested packet or refuses
with one bounded, explainable reason. Every check runs in a fixed,
fail-closed order — an authoritative session/cell, an assignment
freeze, the packet's own identity and tier/effort contract, disjoint
file/worktree ownership against other reserved packets, host resource
headroom — before the ledger's own compare-and-swap reservation is
attempted. No exception ever escapes ``admit``: an unexpected failure
anywhere in the chain is itself a refusal, never a crash that could
leave the caller uncertain whether a subagent was launched. Prompt
text and model output are never read, stored, or logged here.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from hermes_orchestrator.subagent_packets import PacketRefused, SubagentPackets

if TYPE_CHECKING:
    from hermes_orchestrator.db import Database
    from hermes_orchestrator.model_tiers import ModelTier
    from hermes_orchestrator.subagent_packets import SubagentPacket

_VALID_EFFORTS = frozenset({"low", "medium", "high"})


@dataclass(frozen=True, slots=True)
class AdmissionDecision:
    """One explainable admission outcome. ``packet_id`` is set on success."""

    allowed: bool
    reason: str
    packet_id: str | None = None


def _refuse(reason: str) -> AdmissionDecision:
    return AdmissionDecision(allowed=False, reason=reason, packet_id=None)


class PacketAdmission:
    """Gate one Agent-tool subagent launch against the packet ledger."""

    def __init__(
        self,
        database: Database,
        *,
        packets: SubagentPackets,
        tiers: dict[str, ModelTier],
        freeze_dir: Path,
        headroom: Callable[[], bool] = lambda: True,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._database = database
        self._packets = packets
        self._tiers = tiers
        self._freeze_dir = freeze_dir
        self._headroom = headroom
        self._now = now or (lambda: datetime.now(UTC))

    def admit(
        self,
        *,
        session_id: str,
        packet_id: str,
        model: str,
        effort: str,
        tool_use_id: str,
    ) -> AdmissionDecision:
        """Admit or refuse one launch. Never raises."""

        try:
            return self._admit(
                session_id=session_id,
                packet_id=packet_id,
                model=model,
                effort=effort,
                tool_use_id=tool_use_id,
            )
        except Exception:  # admission must fail closed, never raise
            return _refuse("admission failed unexpectedly")

    def _admit(
        self,
        *,
        session_id: str,
        packet_id: str,
        model: str,
        effort: str,
        tool_use_id: str,
    ) -> AdmissionDecision:
        # 1. Authoritative session/cell.
        cell_row = self._database.execute(
            "SELECT cell_id, project_key FROM project_cells "
            "WHERE session_id = ? AND state = 'active'",
            (session_id,),
        ).fetchone()
        if cell_row is None:
            return _refuse("no active cell owns this session")
        cell_id = str(cell_row["cell_id"])
        project_key = str(cell_row["project_key"])

        # 2. Assignment freeze.
        marker = self._freeze_dir / f"{session_id}.frozen"
        if marker.exists():
            reason = marker.read_text(encoding="utf-8").strip() or "rotation_pending"
            return _refuse(reason)

        # 3. Packet exists and belongs to this session's cell.
        try:
            packet = self._packets.get(packet_id)
        except KeyError:
            return _refuse("unknown packet")
        if (
            packet.session_id != session_id
            or packet.cell_id != cell_id
            or packet.project_key != project_key
        ):
            return _refuse("packet does not belong to this session's cell")

        # 4. Requested model/effort must match the packet's bound tier.
        tier = self._tiers.get(packet.model_tier)
        if tier is None or model != tier.model:
            return _refuse(f"packet requires tier {packet.model_tier}")
        if effort not in _VALID_EFFORTS or effort not in (
            packet.effort,
            tier.default_effort,
        ):
            return _refuse(f"packet requires tier {packet.model_tier}")

        # 5. Disjoint ownership against every other reserved packet.
        conflict = self._overlap_conflict(packet)
        if conflict is not None:
            return _refuse(f"overlapping ownership with reserved packet {conflict}")

        # 7. Resource headroom.
        if not self._headroom():
            return _refuse("resource pressure refuses new subagents")

        # 8. The atomic reservation itself; also enforces dependency
        # completion (6) and exactly-once admission.
        try:
            reserved = self._packets.reserve(
                packet_id, session_id=session_id, tool_use_id=tool_use_id
            )
        except PacketRefused as error:
            return _refuse(str(error))

        return AdmissionDecision(
            allowed=True, reason="reserved", packet_id=reserved.packet_id
        )

    def _overlap_conflict(self, packet: SubagentPacket) -> str | None:
        """Return the id of a conflicting reserved packet, if any."""

        rows = self._database.execute(
            "SELECT packet_id, allowed_files_json, worktree "
            "FROM subagent_packets WHERE state = 'reserved' AND packet_id != ?",
            (packet.packet_id,),
        ).fetchall()
        this_files = set(packet.allowed_files)
        this_worktree = packet.worktree
        for row in rows:
            other_worktree = str(row["worktree"])
            other_files = set(json.loads(str(row["allowed_files_json"])))
            if other_worktree == this_worktree or (this_files & other_files):
                return str(row["packet_id"])
        return None
