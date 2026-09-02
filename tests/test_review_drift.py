"""Administrative-only drift must not force wide semantic replay (INFRA-200)."""

from __future__ import annotations

from hermes_orchestrator.review_drift import (
    DriftVerdict,
    classify_drift,
    is_administrative_path,
)

PREV = "a" * 40
CURRENT = "b" * 40


def test_administrative_only_drift_is_classified_without_semantic_replay() -> None:
    verdict = classify_drift(
        previous_tree_sha=PREV,
        current_tree_sha=CURRENT,
        changed_paths=("docs/guide.md", "NOTICE", ".hermes/state.json"),
    )

    assert verdict == DriftVerdict(
        kind="administrative",
        reason="only administrative paths changed",
        changed_paths=("docs/guide.md", "NOTICE", ".hermes/state.json"),
    )
    assert verdict.kind == "administrative"


def test_identical_tree_is_classified_as_no_drift() -> None:
    verdict = classify_drift(
        previous_tree_sha=PREV,
        current_tree_sha=PREV,
        changed_paths=("README.md",),
    )

    assert verdict.kind == "none"
    assert verdict.reason == "identical tree"


def test_first_review_with_no_prior_tree_is_semantic() -> None:
    verdict = classify_drift(
        previous_tree_sha=None,
        current_tree_sha=CURRENT,
        changed_paths=(),
    )

    assert verdict.kind == "semantic"
    assert verdict.reason == "first review"


def test_mixed_administrative_and_code_paths_are_semantic() -> None:
    verdict = classify_drift(
        previous_tree_sha=PREV,
        current_tree_sha=CURRENT,
        changed_paths=("docs/guide.md", "src/hermes_orchestrator/reviews.py"),
    )

    assert verdict.kind == "semantic"


def test_lockfile_only_change_is_never_administrative() -> None:
    verdict = classify_drift(
        previous_tree_sha=PREV,
        current_tree_sha=CURRENT,
        changed_paths=("uv.lock",),
    )

    assert verdict.kind == "semantic"
    assert is_administrative_path("uv.lock") is False


def test_changed_tree_with_no_reported_paths_is_semantic_not_administrative() -> None:
    """No positive evidence of administrative-only change -- fail toward
    semantic rather than vacuously calling zero paths "administrative"."""

    verdict = classify_drift(
        previous_tree_sha=PREV, current_tree_sha=CURRENT, changed_paths=()
    )

    assert verdict.kind == "semantic"


def test_prompts_markdown_is_not_administrative() -> None:
    """A prompt file is semantic instruction, not documentation, even
    though it is written in Markdown."""

    assert is_administrative_path("prompts/codex-merger.md") is False
    assert is_administrative_path("docs/design.md") is True


def test_administrative_path_helpers_cover_the_declared_patterns() -> None:
    assert is_administrative_path("docs/deep/nested/note.txt") is True
    assert is_administrative_path("LICENSE") is True
    assert is_administrative_path("LICENSE.txt") is True
    assert is_administrative_path("CHANGELOG.md") is True
    assert is_administrative_path("NOTICE") is True
    assert is_administrative_path(".hermes/config.json") is True
    assert is_administrative_path("src/hermes_orchestrator/reviews.py") is False
