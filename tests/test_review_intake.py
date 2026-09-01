"""Verify one-candidate wake admission for the Merger review boundary."""

from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from hermes_orchestrator.codex_merger import CodexMerger
from hermes_orchestrator.config import ProjectConfig
from hermes_orchestrator.db import Database
from hermes_orchestrator.manifests import (
    MANIFEST_VERSION,
    CandidateManifest,
    WakeEvent,
    manifest_digest_for,
    read_manifest_snapshot,
    wake_event_for,
    write_manifest,
)
from hermes_orchestrator.review_intake import (
    AdmittedCandidate,
    CandidateAdmission,
    CandidateRejected,
)

HEAD = "1" * 40
BASE = "2" * 40


class NullRpc:
    async def request(
        self, method: str, params: dict[str, Any] | None, timeout: float
    ) -> dict[str, Any]:
        raise AssertionError("admission must not use the RPC surface")


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def merger(database: Database) -> CodexMerger:
    prompt = Path(__file__).parent.parent / "prompts" / "codex-merger.md"
    return CodexMerger(
        rpc=NullRpc(),
        database=database,
        projects={
            "demo": ProjectConfig(
                linear_team="infrastructure",
                repo_path=Path("/repo/demo"),
                integration_branch="main",
                github_repo="j-paterson/demo",
            )
        },
        prompt_file=prompt,
        now=lambda: datetime(2026, 8, 28, tzinfo=UTC),
    )


def stored_channel(database: Database, generation: int = 1) -> None:
    stamp = datetime(2026, 8, 28, tzinfo=UTC).isoformat()
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO reviewer_channels("
            "project_key, thread_id, generation, state, created_at, updated_at"
            ") VALUES ('demo', 'thr_stored', ?, 'ready', ?, ?)",
            (generation, stamp, stamp),
        )


def manifest() -> CandidateManifest:
    return CandidateManifest(
        manifest_version=MANIFEST_VERSION,
        event_id="evt-1",
        status="FABLE_READY",
        candidate_sha=HEAD,
        base_sha=BASE,
        branch="feature/eng-9",
        linear_issues=("ENG-9",),
        changed_files=("src/app.py",),
        verification=(("uv run pytest -q", "142 passed"),),
        blockers=(),
        created_at="2026-08-28T12:00:00+00:00",
    )


def delivered_event(
    merger: CodexMerger,
    tmp_path: Path,
    event_id: str = "evt-1",
    candidate_sha: str = HEAD,
) -> WakeEvent:
    document = manifest()
    if event_id != "evt-1" or candidate_sha != HEAD:
        document = CandidateManifest(
            manifest_version=document.manifest_version,
            event_id=event_id,
            status=document.status,
            candidate_sha=candidate_sha,
            base_sha=document.base_sha,
            branch=document.branch,
            linear_issues=document.linear_issues,
            changed_files=document.changed_files,
            verification=document.verification,
            blockers=document.blockers,
            created_at=document.created_at,
        )
    path = write_manifest(tmp_path, document, head_sha=candidate_sha)
    snapshot = read_manifest_snapshot(path, root=tmp_path)
    event = wake_event_for(document, path)
    registration = merger.register_wake("demo", event, manifest=snapshot)
    assert registration.state == "pending"
    assert registration.claim_token is not None
    assert registration.thread_id is not None
    assert registration.generation is not None
    assert merger.record_wake_delivery_success(
        "demo",
        thread_id=registration.thread_id,
        generation=registration.generation,
        event_id=event.event_id,
        claim_token=registration.claim_token,
        candidate_sha=event.candidate_sha,
    ) is True
    return event


class PassingGate:
    def validate(self, project_key: str, candidate: CandidateManifest) -> None:
        return None


def admission(
    merger: CodexMerger,
    tmp_path: Path,
    head: str = HEAD,
    base_ok: bool = True,
    intake_gate: Any = None,
) -> CandidateAdmission:
    return CandidateAdmission(
        channels=merger,
        manifest_root=tmp_path,
        branch_head=lambda project_key, branch: head,
        base_policy=lambda project_key, base_sha: base_ok,
        intake_gate=intake_gate if intake_gate is not None else PassingGate(),
    )


def test_admits_exactly_one_validated_candidate(
    merger: CodexMerger, database: Database, tmp_path: Path
) -> None:
    stored_channel(database)
    event = delivered_event(merger, tmp_path)

    admitted = admission(merger, tmp_path).admit("demo", event, received_generation=1)

    assert isinstance(admitted, AdmittedCandidate)
    assert admitted.manifest == manifest()
    assert admitted.thread_id == "thr_stored"
    assert admitted.generation == 1
    assert database.scalar(
        "SELECT state FROM wake_deliveries WHERE event_id = 'evt-1'"
    ) == "admitted"

    with pytest.raises(CandidateRejected, match="one"):
        admission(merger, tmp_path).admit("demo", event, received_generation=1)


def test_rejects_a_stale_generation_without_state_change(
    merger: CodexMerger, database: Database, tmp_path: Path
) -> None:
    stored_channel(database, generation=3)
    event = delivered_event(merger, tmp_path)

    with pytest.raises(CandidateRejected, match="generation"):
        admission(merger, tmp_path).admit("demo", event, received_generation=1)

    assert database.scalar(
        "SELECT state FROM wake_deliveries WHERE event_id = 'evt-1'"
    ) == "delivered"


def test_rejects_wake_and_manifest_disagreement(
    merger: CodexMerger, database: Database, tmp_path: Path
) -> None:
    stored_channel(database)
    delivered_event(merger, tmp_path)
    mismatched = WakeEvent(
        status="FABLE_READY",
        issue_id="ENG-9",
        candidate_sha="3" * 40,
        base_sha=BASE,
        manifest_path=str(tmp_path / "evt-1.json"),
        event_id="evt-1",
        manifest_digest=manifest_digest_for(manifest()),
    )

    with pytest.raises(CandidateRejected, match="manifest"):
        admission(merger, tmp_path).admit("demo", mismatched, received_generation=1)

    assert database.scalar(
        "SELECT state FROM wake_deliveries WHERE event_id = 'evt-1'"
    ) == "delivered"


def test_rejects_a_candidate_that_is_not_the_branch_head(
    merger: CodexMerger, database: Database, tmp_path: Path
) -> None:
    stored_channel(database)
    event = delivered_event(merger, tmp_path)

    with pytest.raises(CandidateRejected, match="head"):
        admission(merger, tmp_path, head="4" * 40).admit(
            "demo", event, received_generation=1
        )
    assert database.scalar(
        "SELECT state FROM wake_deliveries WHERE event_id = 'evt-1'"
    ) == "delivered"


def test_rejects_a_stale_base_by_policy(
    merger: CodexMerger, database: Database, tmp_path: Path
) -> None:
    stored_channel(database)
    event = delivered_event(merger, tmp_path)

    with pytest.raises(CandidateRejected, match="base"):
        admission(merger, tmp_path, base_ok=False).admit(
            "demo", event, received_generation=1
        )


def test_rejects_a_mutable_manifest(
    merger: CodexMerger, database: Database, tmp_path: Path
) -> None:
    stored_channel(database)
    event = delivered_event(merger, tmp_path)
    (tmp_path / "evt-1.json").chmod(0o644)

    with pytest.raises(CandidateRejected, match="mutable"):
        admission(merger, tmp_path).admit("demo", event, received_generation=1)


def test_admission_requires_base_policy_and_intake_gate(
    merger: CodexMerger, tmp_path: Path
) -> None:
    with pytest.raises(TypeError):
        CandidateAdmission(  # type: ignore[call-arg]
            channels=merger,
            manifest_root=tmp_path,
            branch_head=lambda project_key, branch: HEAD,
        )
    with pytest.raises(ValueError, match="base policy"):
        CandidateAdmission(
            channels=merger,
            manifest_root=tmp_path,
            branch_head=lambda project_key, branch: HEAD,
            base_policy=None,  # type: ignore[arg-type]
            intake_gate=PassingGate(),
        )
    with pytest.raises(ValueError, match="intake gate"):
        CandidateAdmission(
            channels=merger,
            manifest_root=tmp_path,
            branch_head=lambda project_key, branch: HEAD,
            base_policy=lambda project_key, base_sha: True,
            intake_gate=None,  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="intake gate"):
        CandidateAdmission(
            channels=merger,
            manifest_root=tmp_path,
            branch_head=lambda project_key, branch: HEAD,
            base_policy=lambda project_key, base_sha: True,
            intake_gate=object(),  # type: ignore[arg-type]
        )


def test_validators_run_in_order_before_the_admission_cas(
    merger: CodexMerger,
    database: Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored_channel(database)
    event = delivered_event(merger, tmp_path)
    order: list[str] = []

    class RecordingGate:
        def validate(
            self, project_key: str, candidate: CandidateManifest
        ) -> None:
            order.append("gate")

    def recording_policy(project_key: str, base_sha: str) -> bool:
        order.append("base_policy")
        return True

    real_admit = merger.admit_wake

    def recording_admit(*args: Any, **kwargs: Any) -> bool:
        order.append("admit_cas")
        return real_admit(*args, **kwargs)

    monkeypatch.setattr(merger, "admit_wake", recording_admit)

    gate = CandidateAdmission(
        channels=merger,
        manifest_root=tmp_path,
        branch_head=lambda project_key, branch: HEAD,
        base_policy=recording_policy,
        intake_gate=RecordingGate(),
    )
    gate.admit("demo", event, received_generation=1)

    assert order == ["base_policy", "gate", "admit_cas"]


def test_rejects_manifests_outside_the_configured_root(
    merger: CodexMerger, database: Database, tmp_path: Path
) -> None:
    stored_channel(database)
    root = tmp_path / "root"
    root.mkdir()
    event = delivered_event(merger, tmp_path)

    with pytest.raises(CandidateRejected, match="root"):
        admission(merger, root).admit("demo", event, received_generation=1)


def test_intake_gate_runs_before_admission_and_fails_closed(
    merger: CodexMerger, database: Database, tmp_path: Path
) -> None:
    stored_channel(database)
    event = delivered_event(merger, tmp_path)
    seen: list[tuple[str, str]] = []

    class RejectingGate:
        def validate(
            self, project_key: str, candidate: CandidateManifest
        ) -> None:
            seen.append((project_key, candidate.event_id))
            raise CandidateRejected(
                "previous pull request CI is unreconciled"
            )

    with pytest.raises(CandidateRejected, match="unreconciled"):
        admission(merger, tmp_path, intake_gate=RejectingGate()).admit(
            "demo", event, received_generation=1
        )

    assert seen == [("demo", "evt-1")]
    assert database.scalar(
        "SELECT state FROM wake_deliveries WHERE event_id = 'evt-1'"
    ) == "delivered"


def test_replacement_during_admission_never_admits_the_old_thread(
    merger: CodexMerger, database: Database, tmp_path: Path
) -> None:
    stored_channel(database)
    event = delivered_event(merger, tmp_path)

    class ReplacingGate:
        def validate(
            self, project_key: str, candidate: CandidateManifest
        ) -> None:
            merger.begin_replacement(
                "demo",
                expected_thread_id="thr_stored",
                expected_generation=1,
                reason="thread lost",
            )
            merger.complete_replacement(
                "demo",
                expected_thread_id="thr_stored",
                expected_generation=1,
                new_thread_id="thr_new",
            )

    with pytest.raises(CandidateRejected, match="one"):
        admission(merger, tmp_path, intake_gate=ReplacingGate()).admit(
            "demo", event, received_generation=1
        )

    row = database.scalar(
        "SELECT state FROM wake_deliveries WHERE event_id = 'evt-1'"
    )
    assert row == "delivered"


def test_concurrent_candidates_cannot_both_be_admitted(
    merger: CodexMerger, database: Database, tmp_path: Path
) -> None:
    stored_channel(database)
    first = delivered_event(merger, tmp_path)
    second = delivered_event(
        merger, tmp_path, event_id="evt-2", candidate_sha="5" * 40
    )

    admitted = admission(merger, tmp_path).admit(
        "demo", first, received_generation=1
    )
    assert admitted.manifest.event_id == "evt-1"

    gate = admission(merger, tmp_path, head="5" * 40)
    with pytest.raises(CandidateRejected, match="one"):
        gate.admit("demo", second, received_generation=1)

    assert merger.complete_admitted_wake("demo", "evt-1") is True
    released = gate.admit("demo", second, received_generation=1)
    assert released.manifest.event_id == "evt-2"


def test_admission_cannot_transition_a_divergent_delivered_row(
    merger: CodexMerger, database: Database, tmp_path: Path
) -> None:
    stored_channel(database)
    delivered_event(merger, tmp_path)

    path = tmp_path / "evt-1.json"
    os.chmod(path, 0o644)
    path.unlink()
    replacement = CandidateManifest(
        manifest_version=MANIFEST_VERSION,
        event_id="evt-1",
        status="FABLE_READY",
        candidate_sha="3" * 40,
        base_sha=BASE,
        branch="feature/eng-9",
        linear_issues=("ENG-9",),
        changed_files=("src/app.py",),
        verification=(("uv run pytest -q", "142 passed"),),
        blockers=(),
        created_at="2026-08-28T12:00:00+00:00",
    )
    write_manifest(tmp_path, replacement, head_sha="3" * 40)
    forged = wake_event_for(replacement, path)

    with pytest.raises(CandidateRejected, match="one"):
        admission(merger, tmp_path, head="3" * 40).admit(
            "demo", forged, received_generation=1
        )

    assert database.scalar(
        "SELECT state FROM wake_deliveries WHERE event_id = 'evt-1'"
    ) == "delivered"
    assert database.scalar(
        "SELECT candidate_sha FROM wake_deliveries WHERE event_id = 'evt-1'"
    ) == HEAD


def test_stale_generation_delivery_cannot_be_admitted_as_new(
    merger: CodexMerger, database: Database, tmp_path: Path
) -> None:
    stored_channel(database)
    event = delivered_event(merger, tmp_path)

    merger.begin_replacement(
        "demo",
        expected_thread_id="thr_stored",
        expected_generation=1,
        reason="thread lost",
    )
    merger.complete_replacement(
        "demo",
        expected_thread_id="thr_stored",
        expected_generation=1,
        new_thread_id="thr_new",
    )

    with pytest.raises(CandidateRejected, match="one"):
        admission(merger, tmp_path).admit(
            "demo", event, received_generation=2
        )

    assert database.scalar(
        "SELECT state FROM wake_deliveries WHERE event_id = 'evt-1'"
    ) == "delivered"


def test_identical_byte_replacement_blocks_admission(
    merger: CodexMerger, database: Database, tmp_path: Path
) -> None:
    stored_channel(database)
    event = delivered_event(merger, tmp_path)

    path = tmp_path / "evt-1.json"
    raw = path.read_bytes()
    os.chmod(path, 0o644)
    path.unlink()
    path.write_bytes(raw)
    os.chmod(path, 0o444)

    with pytest.raises(CandidateRejected, match="one"):
        admission(merger, tmp_path).admit(
            "demo", event, received_generation=1
        )

    assert database.scalar(
        "SELECT state FROM wake_deliveries WHERE event_id = 'evt-1'"
    ) == "delivered"


def test_rejects_an_undelivered_event(
    merger: CodexMerger, database: Database, tmp_path: Path
) -> None:
    stored_channel(database)
    path = write_manifest(tmp_path, manifest(), head_sha=HEAD)
    snapshot = read_manifest_snapshot(path, root=tmp_path)
    event = wake_event_for(manifest(), path)
    merger.register_wake("demo", event, manifest=snapshot)

    with pytest.raises(CandidateRejected, match="one"):
        admission(merger, tmp_path).admit("demo", event, received_generation=1)


def test_a_deleted_branch_admits_only_with_a_proven_merged_pull(
    merger: CodexMerger, database: Database, tmp_path: Path
) -> None:
    """INFRA-217: GitHub deletes a merged candidate's branch.

    ``branch_head`` then resolves to "" and admission used to reject a
    candidate whose merge was already PROVEN — the production defect Sol
    found. An unresolvable branch is now excused ONLY by a proof that
    this exact reviewed SHA is an already-merged same-repository pull
    request targeting the integration branch.
    """

    stored_channel(database)
    event = delivered_event(merger, tmp_path)
    calls: list[tuple[str, str, str]] = []

    def proof(project_key: str, branch: str, candidate_sha: str) -> bool:
        calls.append((project_key, branch, candidate_sha))
        return True

    admitted = CandidateAdmission(
        channels=merger,
        manifest_root=tmp_path,
        branch_head=lambda project_key, branch: "",
        base_policy=lambda project_key, base_sha: True,
        intake_gate=PassingGate(),
        merged_candidate_proof=proof,
    ).admit("demo", event, received_generation=1)

    assert admitted.manifest.candidate_sha == HEAD
    # The proof was asked about this exact candidate, not a guess.
    assert calls == [("demo", admitted.manifest.branch, HEAD)]


def test_a_deleted_branch_without_merge_proof_still_fails_closed(
    merger: CodexMerger, database: Database, tmp_path: Path
) -> None:
    """No proof, or a proof that refuses, keeps today's rejection."""

    stored_channel(database)
    event = delivered_event(merger, tmp_path)

    with pytest.raises(CandidateRejected, match="current branch head"):
        admission(merger, tmp_path, head="").admit(
            "demo", event, received_generation=1
        )

    with pytest.raises(CandidateRejected, match="current branch head"):
        CandidateAdmission(
            channels=merger,
            manifest_root=tmp_path,
            branch_head=lambda project_key, branch: "",
            base_policy=lambda project_key, base_sha: True,
            intake_gate=PassingGate(),
            merged_candidate_proof=lambda *args: False,
        ).admit("demo", event, received_generation=1)


def test_a_resolving_branch_at_another_commit_is_never_excused(
    merger: CodexMerger, database: Database, tmp_path: Path
) -> None:
    """Exact-head validation for OPEN candidates is preserved.

    A branch that resolves to a DIFFERENT commit is a stale candidate,
    and no merge proof may excuse it — the proof is never even consulted.
    """

    stored_channel(database)
    event = delivered_event(merger, tmp_path)
    consulted: list[object] = []

    def proof(project_key: str, branch: str, candidate_sha: str) -> bool:
        consulted.append((project_key, branch, candidate_sha))
        return True

    with pytest.raises(CandidateRejected, match="current branch head"):
        CandidateAdmission(
            channels=merger,
            manifest_root=tmp_path,
            branch_head=lambda project_key, branch: "f" * 40,
            base_policy=lambda project_key, base_sha: True,
            intake_gate=PassingGate(),
            merged_candidate_proof=proof,
        ).admit("demo", event, received_generation=1)

    assert consulted == []
