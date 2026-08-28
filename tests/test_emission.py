"""Verify freeze-boundary candidate emission: manifest plus queue wake."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hermes_orchestrator.codex_queue import QueueDeliveryResult
from hermes_orchestrator.config import ProjectConfig
from hermes_orchestrator.emission import CandidateEmitter, EmissionBlocked
from hermes_orchestrator.git import GitResult
from hermes_orchestrator.manifests import WakeEvent, read_manifest

HEAD = "1" * 40
BASE = "4" * 40
NOW = datetime(2026, 8, 28, 12, tzinfo=UTC)


@dataclass
class FakeGitRunner:
    """Argv-keyed git fake; unknown invocations fail like a broken clone."""

    responses: dict[tuple[str, ...], str] = field(default_factory=dict)
    calls: list[tuple[str, ...]] = field(default_factory=list)

    def run(self, args: tuple[str, ...], cwd: Path) -> GitResult:
        self.calls.append(args)
        key = args[1:]
        if key not in self.responses:
            return GitResult(128, "", "fatal")
        return GitResult(0, self.responses[key], "")


@dataclass
class FakeDeliverer:
    events: list[tuple[str, WakeEvent]] = field(default_factory=list)

    async def deliver(self, project_key: str, event: WakeEvent) -> QueueDeliveryResult:
        self.events.append((project_key, event))
        return QueueDeliveryResult(
            delivered=True,
            attempts=1,
            thread_id="thr_demo",
            generation=1,
            reason="delivered",
        )


def clean_git(head: str = HEAD, branch: str = "feature/eng-9") -> FakeGitRunner:
    return FakeGitRunner(
        responses={
            ("status", "--porcelain"): "",
            ("rev-parse", "HEAD"): head + "\n",
            ("rev-parse", "--abbrev-ref", "HEAD"): branch + "\n",
            ("fetch", "--", "origin", "main"): "",
            ("fetch", "--", "origin", branch): "",
            ("rev-parse", f"origin/{branch}"): head + "\n",
            ("merge-base", "HEAD", "origin/main"): BASE + "\n",
            ("diff", "--name-only", BASE, head): "src/app.py\ntests/test_app.py\n",
        }
    )


@pytest.fixture
def emitter_parts(
    tmp_path: Path,
) -> tuple[CandidateEmitter, FakeGitRunner, FakeDeliverer]:
    git = clean_git()
    deliverer = FakeDeliverer()
    root = tmp_path / "manifests"
    root.mkdir()
    emitter = CandidateEmitter(
        projects={
            "demo": ProjectConfig(
                linear_team="infrastructure",
                repo_path=tmp_path,
                integration_branch="main",
                github_repo="j-paterson/demo",
            )
        },
        git=git,
        manifest_root=root,
        delivery=deliverer,
        now=lambda: NOW,
    )
    return emitter, git, deliverer


@pytest.mark.asyncio
async def test_emits_immutable_manifest_and_delivers_typed_wake(
    emitter_parts: tuple[CandidateEmitter, FakeGitRunner, FakeDeliverer],
    tmp_path: Path,
) -> None:
    emitter, git, deliverer = emitter_parts
    result = await emitter.emit(
        "demo", "ENG-9", verification=(("uv run pytest -q", "564 passed"),)
    )
    assert result.delivery.delivered is True
    assert result.reused_manifest is False
    assert result.event.status == "FABLE_READY"
    assert result.event.issue_id == "ENG-9"
    assert result.event.candidate_sha == HEAD
    assert result.event.base_sha == BASE
    assert result.event.event_id == f"fable_ready-eng-9-{HEAD[:12]}-20260828T120000"
    manifest = read_manifest(result.manifest_path, root=tmp_path / "manifests")
    assert manifest.branch == "feature/eng-9"
    assert manifest.changed_files == ("src/app.py", "tests/test_app.py")
    assert manifest.verification == (("uv run pytest -q", "564 passed"),)
    assert deliverer.events == [("demo", result.event)]
    assert ("git", "fetch", "--", "origin", "feature/eng-9") in git.calls


@pytest.mark.asyncio
async def test_re_emission_reuses_the_immutable_manifest(
    emitter_parts: tuple[CandidateEmitter, FakeGitRunner, FakeDeliverer],
) -> None:
    emitter, _, deliverer = emitter_parts
    first = await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))
    second = await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))
    assert second.reused_manifest is True
    assert second.event == first.event
    assert len(deliverer.events) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({("status", "--porcelain"): " M src/app.py\n"}, "not clean"),
        ({("rev-parse", "origin/feature/eng-9"): "2" * 40 + "\n"}, "pushed branch"),
        ({("rev-parse", "--abbrev-ref", "HEAD"): "main\n"}, "feature branch"),
        ({("diff", "--name-only", BASE, HEAD): "\n"}, "no changes"),
    ],
)
async def test_invalid_freeze_boundaries_fail_closed(
    emitter_parts: tuple[CandidateEmitter, FakeGitRunner, FakeDeliverer],
    override: dict[tuple[str, ...], str],
    message: str,
) -> None:
    emitter, git, deliverer = emitter_parts
    git.responses.update(override)
    with pytest.raises(EmissionBlocked, match=message):
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))
    assert deliverer.events == []


@pytest.mark.asyncio
async def test_git_failure_and_missing_verification_fail_closed(
    emitter_parts: tuple[CandidateEmitter, FakeGitRunner, FakeDeliverer],
) -> None:
    emitter, git, deliverer = emitter_parts
    with pytest.raises(EmissionBlocked, match="verification"):
        await emitter.emit("demo", "ENG-9", verification=())
    del git.responses[("fetch", "--", "origin", "main")]
    with pytest.raises(EmissionBlocked, match="git fetch failed"):
        await emitter.emit("demo", "ENG-9", verification=(("pytest", "ok"),))
    with pytest.raises(EmissionBlocked, match="unknown project"):
        await emitter.emit("nope", "ENG-9", verification=(("pytest", "ok"),))
    assert deliverer.events == []
