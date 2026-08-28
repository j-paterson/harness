"""Verify the immutable candidate manifest writer and strict reader."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

from hermes_orchestrator import manifests
from hermes_orchestrator.manifests import (
    MANIFEST_VERSION,
    CandidateManifest,
    ManifestError,
    WakeEvent,
    manifest_digest_for,
    read_manifest,
    wake_event_for,
    write_manifest,
)

HEAD = "1" * 40
BASE = "2" * 40


def manifest(**overrides: Any) -> CandidateManifest:
    fields: dict[str, Any] = {
        "manifest_version": MANIFEST_VERSION,
        "event_id": "evt-1",
        "status": "FABLE_READY",
        "candidate_sha": HEAD,
        "base_sha": BASE,
        "branch": "feature/eng-9",
        "linear_issues": ("ENG-9",),
        "changed_files": ("src/app.py", "tests/test_app.py"),
        "verification": (("uv run pytest -q", "142 passed"),),
        "blockers": (),
        "created_at": "2026-08-28T12:00:00+00:00",
    }
    fields.update(overrides)
    return CandidateManifest(**fields)


def payload(**overrides: Any) -> dict[str, Any]:
    document: dict[str, Any] = {
        "manifestVersion": MANIFEST_VERSION,
        "eventId": "evt-1",
        "status": "FABLE_READY",
        "candidateSha": HEAD,
        "baseSha": BASE,
        "branch": "feature/eng-9",
        "linearIssues": ["ENG-9"],
        "changedFiles": ["src/app.py", "tests/test_app.py"],
        "verification": [
            {"command": "uv run pytest -q", "outcome": "142 passed"}
        ],
        "blockers": [],
        "createdAt": "2026-08-28T12:00:00+00:00",
    }
    document.update(overrides)
    return document


def write_raw(path: Path, document: dict[str, Any]) -> Path:
    path.write_text(json.dumps(document), encoding="utf-8")
    os.chmod(path, 0o444)
    return path


def test_write_then_read_round_trips_and_is_immutable(tmp_path: Path) -> None:
    path = write_manifest(tmp_path, manifest(), head_sha=HEAD)

    assert path == tmp_path / "evt-1.json"
    assert not os.access(path, os.W_OK)
    assert [entry.name for entry in tmp_path.iterdir()] == ["evt-1.json"]

    loaded = read_manifest(path, root=tmp_path)
    assert loaded == manifest()


def test_write_refuses_a_candidate_that_is_not_head(tmp_path: Path) -> None:
    with pytest.raises(ManifestError, match="HEAD"):
        write_manifest(tmp_path, manifest(), head_sha="3" * 40)
    assert list(tmp_path.iterdir()) == []


def test_write_refuses_to_replace_an_existing_manifest(tmp_path: Path) -> None:
    write_manifest(tmp_path, manifest(), head_sha=HEAD)
    with pytest.raises(ManifestError, match="immutable"):
        write_manifest(tmp_path, manifest(), head_sha=HEAD)


def test_reader_rejects_a_writable_manifest(tmp_path: Path) -> None:
    path = tmp_path / "evt-1.json"
    path.write_text(json.dumps(payload()), encoding="utf-8")

    with pytest.raises(ManifestError, match="mutable"):
        read_manifest(path, root=tmp_path)


def test_reader_rejects_unknown_versions(tmp_path: Path) -> None:
    path = write_raw(tmp_path / "evt-1.json", payload(manifestVersion=2))
    with pytest.raises(ManifestError, match="version"):
        read_manifest(path, root=tmp_path)


def test_reader_rejects_missing_and_extra_fields(tmp_path: Path) -> None:
    missing = payload()
    del missing["baseSha"]
    path = write_raw(tmp_path / "missing.json", missing)
    with pytest.raises(ManifestError, match="baseSha"):
        read_manifest(path, root=tmp_path)

    extra = payload(surprise=True)
    path = write_raw(tmp_path / "extra.json", extra)
    with pytest.raises(ManifestError, match="surprise"):
        read_manifest(path, root=tmp_path)


def test_reader_rejects_invalid_shas(tmp_path: Path) -> None:
    for field in ("candidateSha", "baseSha"):
        path = write_raw(
            tmp_path / f"{field}.json", payload(**{field: "not-a-sha"})
        )
        with pytest.raises(ManifestError, match="sha"):
            read_manifest(path, root=tmp_path)


def test_reader_rejects_unknown_statuses(tmp_path: Path) -> None:
    path = write_raw(tmp_path / "evt-1.json", payload(status="HEARTBEAT"))
    with pytest.raises(ManifestError, match="status"):
        read_manifest(path, root=tmp_path)


def test_reader_rejects_duplicate_files_and_issues(tmp_path: Path) -> None:
    path = write_raw(
        tmp_path / "files.json",
        payload(changedFiles=["src/app.py", "src/app.py"]),
    )
    with pytest.raises(ManifestError, match="duplicate"):
        read_manifest(path, root=tmp_path)

    path = write_raw(
        tmp_path / "issues.json", payload(linearIssues=["ENG-9", "ENG-9"])
    )
    with pytest.raises(ManifestError, match="duplicate"):
        read_manifest(path, root=tmp_path)


def test_reader_rejects_path_traversal(tmp_path: Path) -> None:
    for bad in ("../secrets.txt", "/etc/passwd", "src/../../x.py"):
        path = write_raw(
            tmp_path / "evt-1.json", payload(changedFiles=[bad])
        )
        with pytest.raises(ManifestError, match="path"):
            read_manifest(path, root=tmp_path)
        os.chmod(path, 0o644)
        path.unlink()


def test_ready_manifests_require_issues_files_and_verification(
    tmp_path: Path,
) -> None:
    for status in ("FABLE_READY", "FABLE_REWORK_READY"):
        for field, empty in (
            ("linearIssues", []),
            ("changedFiles", []),
            ("verification", []),
        ):
            path = write_raw(
                tmp_path / "evt-1.json",
                payload(status=status, **{field: empty}),
            )
            with pytest.raises(ManifestError, match=field):
                read_manifest(path, root=tmp_path)
            os.chmod(path, 0o644)
            path.unlink()


def test_blocked_manifests_require_blockers_for_empty_files(
    tmp_path: Path,
) -> None:
    invalid = write_raw(
        tmp_path / "invalid.json",
        payload(status="FABLE_BLOCKED", changedFiles=[], blockers=[]),
    )
    with pytest.raises(ManifestError, match="blockers"):
        read_manifest(invalid, root=tmp_path)

    valid = write_raw(
        tmp_path / "evt-1.json",
        payload(
            status="FABLE_BLOCKED",
            changedFiles=[],
            blockers=["sandbox denies network access"],
        ),
    )
    loaded = read_manifest(valid, root=tmp_path)
    assert loaded.status == "FABLE_BLOCKED"
    assert loaded.changed_files == ()


def test_complete_manifests_require_verification_for_empty_files(
    tmp_path: Path,
) -> None:
    invalid = write_raw(
        tmp_path / "invalid.json",
        payload(status="FABLE_COMPLETE", changedFiles=[], verification=[]),
    )
    with pytest.raises(ManifestError, match="verification"):
        read_manifest(invalid, root=tmp_path)

    valid = write_raw(
        tmp_path / "evt-1.json",
        payload(status="FABLE_COMPLETE", changedFiles=[]),
    )
    assert read_manifest(valid, root=tmp_path).status == "FABLE_COMPLETE"


def test_reader_rejects_malformed_verification_entries(
    tmp_path: Path,
) -> None:
    for entry in (
        {"command": "uv run pytest -q"},
        {"command": "x", "outcome": "y", "extra": "z"},
        {"command": "", "outcome": "ok"},
        "uv run pytest -q",
    ):
        path = write_raw(
            tmp_path / "evt-1.json", payload(verification=[entry])
        )
        with pytest.raises(ManifestError, match="verification"):
            read_manifest(path, root=tmp_path)
        os.chmod(path, 0o644)
        path.unlink()


def test_reader_rejects_invalid_timestamps(tmp_path: Path) -> None:
    path = write_raw(
        tmp_path / "evt-1.json", payload(createdAt="yesterday-ish")
    )
    with pytest.raises(ManifestError, match="createdAt"):
        read_manifest(path, root=tmp_path)


def test_event_ids_follow_a_strict_grammar(tmp_path: Path) -> None:
    for bad in ("../evt", "evt/1", "evt 1", ".evt", "", "e" * 80):
        with pytest.raises(ManifestError, match="eventId"):
            write_manifest(
                tmp_path, manifest(event_id=bad), head_sha=HEAD
            )
    assert list(tmp_path.iterdir()) == []


def test_reader_rejects_filenames_that_do_not_match_the_event_id(
    tmp_path: Path,
) -> None:
    path = write_raw(tmp_path / "evt-2.json", payload())
    with pytest.raises(ManifestError, match="filename"):
        read_manifest(path, root=tmp_path)


def test_reader_rejects_manifests_outside_the_state_root(
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    path = write_raw(outside / "evt-1.json", payload())

    with pytest.raises(ManifestError, match="root"):
        read_manifest(path, root=root)


def test_reader_rejects_symlinked_manifests(tmp_path: Path) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    target = write_raw(outside / "evt-1.json", payload())
    link = root / "evt-1.json"
    link.symlink_to(target)

    with pytest.raises(ManifestError, match=r"symlink|regular|unreadable"):
        read_manifest(link, root=root)


def test_writer_leaves_no_temporary_file_on_failure(tmp_path: Path) -> None:
    write_manifest(tmp_path, manifest(), head_sha=HEAD)
    with pytest.raises(ManifestError, match="immutable"):
        write_manifest(tmp_path, manifest(), head_sha=HEAD)
    assert [entry.name for entry in tmp_path.iterdir()] == ["evt-1.json"]


def test_all_wakeable_statuses_require_a_linear_issue(tmp_path: Path) -> None:
    for status in (
        "FABLE_READY",
        "FABLE_REWORK_READY",
        "FABLE_BLOCKED",
        "FABLE_COMPLETE",
    ):
        document = payload(status=status, linearIssues=[])
        if status == "FABLE_BLOCKED":
            document["blockers"] = ["sandbox denies network access"]
        path = write_raw(tmp_path / "evt-1.json", document)
        with pytest.raises(ManifestError, match="linearIssues"):
            read_manifest(path, root=tmp_path)
        os.chmod(path, 0o644)
        path.unlink()


def test_reader_rejects_non_linear_issue_identifiers(tmp_path: Path) -> None:
    for bad in (" ", "  ENG-9", "eng-9", "ENG-0", "E-1", "ENG9", "9-ENG"):
        path = write_raw(
            tmp_path / "evt-1.json", payload(linearIssues=[bad])
        )
        with pytest.raises(ManifestError, match=r"Linear|linearIssues"):
            read_manifest(path, root=tmp_path)
        os.chmod(path, 0o644)
        path.unlink()


def test_wake_events_fail_closed_on_loose_grammar(tmp_path: Path) -> None:
    path = write_manifest(tmp_path, manifest(), head_sha=HEAD)
    valid = wake_event_for(manifest(), path)

    for override, expectation in (
        ({"issue_id": " "}, "Linear"),
        ({"issue_id": "eng-9"}, "Linear"),
        ({"candidate_sha": "abc"}, "sha"),
        ({"base_sha": ""}, "sha"),
        ({"event_id": "../evt"}, "id"),
        ({"manifest_digest": "zz"}, "digest"),
    ):
        fields = {
            "status": valid.status,
            "issue_id": valid.issue_id,
            "candidate_sha": valid.candidate_sha,
            "base_sha": valid.base_sha,
            "manifest_path": valid.manifest_path,
            "event_id": valid.event_id,
            "manifest_digest": valid.manifest_digest,
        }
        fields.update(override)
        with pytest.raises(ValueError, match=expectation):
            WakeEvent(**fields)


def test_temp_replacement_at_the_chmod_boundary_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def attack(descriptor: int, mode: int) -> None:
        os.fchmod(descriptor, mode)
        for entry in tmp_path.iterdir():
            if entry.name.startswith(".manifest-"):
                entry.unlink()
                entry.write_text(
                    json.dumps(payload(changedFiles=["evil.py"])),
                    encoding="utf-8",
                )

    monkeypatch.setattr(manifests, "_fchmod", attack)

    with pytest.raises(ManifestError, match="replaced"):
        write_manifest(tmp_path, manifest(), head_sha=HEAD)
    assert not (tmp_path / "evt-1.json").exists()


def test_mode_flip_during_the_current_stat_check_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = write_manifest(tmp_path, manifest(), head_sha=HEAD)
    real_stat = os.stat

    def flipping_stat(target: Any, **kwargs: Any) -> os.stat_result:
        os.chmod(target, 0o644)
        return real_stat(target, **kwargs)

    monkeypatch.setattr(manifests, "_stat_path", flipping_stat)

    with pytest.raises(ManifestError, match="identity"):
        read_manifest(path, root=tmp_path)


def test_replacement_before_open_is_rejected_by_the_durable_digest(
    tmp_path: Path,
) -> None:
    path = write_manifest(tmp_path, manifest(), head_sha=HEAD)
    event = wake_event_for(manifest(), path)

    os.chmod(path, 0o644)
    path.unlink()
    substituted = manifest(changed_files=("evil.py",))
    write_manifest(tmp_path, substituted, head_sha=HEAD)

    with pytest.raises(ManifestError, match="digest"):
        read_manifest(
            path, root=tmp_path, expected_digest=event.manifest_digest
        )


def test_identical_bytes_at_a_new_inode_fail_the_identity_check(
    tmp_path: Path,
) -> None:
    path = write_manifest(tmp_path, manifest(), head_sha=HEAD)
    snapshot = manifests.read_manifest_snapshot(path, root=tmp_path)

    raw = path.read_bytes()
    os.chmod(path, 0o644)
    path.unlink()
    path.write_bytes(raw)
    os.chmod(path, 0o444)

    replaced = manifests.read_manifest_snapshot(
        path, root=tmp_path, expected_digest=snapshot.digest
    )
    assert replaced.digest == snapshot.digest
    assert replaced.identity != snapshot.identity

    with pytest.raises(ManifestError, match="identity"):
        manifests.read_manifest_snapshot(
            path,
            root=tmp_path,
            expected_digest=snapshot.digest,
            expected_identity=snapshot.identity,
        )


def test_manifest_digest_matches_the_published_bytes(tmp_path: Path) -> None:
    path = write_manifest(tmp_path, manifest(), head_sha=HEAD)
    digest = manifest_digest_for(manifest())

    loaded = read_manifest(path, root=tmp_path, expected_digest=digest)
    assert loaded == manifest()


def test_wake_event_for_derives_the_envelope(tmp_path: Path) -> None:
    path = write_manifest(tmp_path, manifest(), head_sha=HEAD)

    event = wake_event_for(manifest(), path)

    assert event.status == "FABLE_READY"
    assert event.event_id == "evt-1"
    assert event.issue_id == "ENG-9"
    assert event.candidate_sha == HEAD
    assert event.base_sha == BASE
    assert event.manifest_path == str(path)
    assert event.render(3).endswith("generation=3")
