"""Pure classifier: administrative-only drift vs semantic drift (INFRA-200).

Stale METADATA about a candidate -- a documentation edit, a NOTICE/LICENSE
update, a prompt-adjacent markdown file outside ``prompts/`` -- must not
force a wide semantic replay or invalidate correct output that was already
proven against verifier attestations, PR/CI/preview/QA evidence,
reviewer-fix receipts, and the submitted-to-merged SHA mapping. At the same
time, candidate/base/tree identity and durable packet integrity must still
fail closed -- this module never decides admission; it only classifies
what changed so a caller MAY scope a replay narrowly. See
``review_intake.py`` for how (and how little) that classification is
allowed to matter.

This module is intentionally pure and dependency-free: it takes tree SHAs
and a list of changed paths and returns a verdict, with no I/O, no
database, and no knowledge of Fable, Sol, or any wake envelope.
"""

from __future__ import annotations

import fnmatch
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

DriftClass = Literal["semantic", "administrative", "none"]

#: Conservative allow-list of path patterns that, when they are the ONLY
#: thing that changed, count as administrative-only drift. Deliberately
#: narrow: a lockfile (``uv.lock``, ``package-lock.json``, ``poetry.lock``,
#: ...) is NOT here -- a dependency pin can change runtime behavior, so a
#: lockfile-only change is always semantic. Matching is against a POSIX
#: relative path with any leading ``/`` stripped; ``**`` patterns match
#: the named directory and everything under it.
ADMINISTRATIVE_PATH_PATTERNS: tuple[str, ...] = (
    "docs/**",
    "*.md",
    ".hermes/**",
    "NOTICE",
    "LICENSE*",
    "CHANGELOG*",
)

#: ``*.md`` is administrative EVERYWHERE except under this prefix: a
#: prompt file is semantic content -- it is reviewer/agent instruction,
#: not documentation -- even though it happens to be written in Markdown.
_PROMPTS_PREFIX = "prompts/"


@dataclass(frozen=True, slots=True)
class DriftVerdict:
    """One classification outcome; never itself an admission decision."""

    kind: DriftClass
    reason: str
    changed_paths: tuple[str, ...]


def is_administrative_path(path: str) -> bool:
    """True iff one changed path, on its own, is administrative-only.

    A markdown file counts UNLESS it lives under ``prompts/`` (prompts are
    semantic, not documentation, regardless of extension). Every other
    path is tested against :data:`ADMINISTRATIVE_PATH_PATTERNS`, with a
    ``**`` suffix matching the named directory and everything beneath it.
    """

    normalized = path.lstrip("/")
    if normalized.startswith(_PROMPTS_PREFIX):
        # A prompt is semantic instruction, never administrative, no
        # matter which pattern below would otherwise match it.
        return False
    if normalized.endswith(".md"):
        return True
    for pattern in ADMINISTRATIVE_PATH_PATTERNS:
        if pattern == "*.md":
            # Already handled, prompts-aware, above.
            continue
        if pattern.endswith("/**"):
            prefix = pattern[: -len("**")]
            if normalized.startswith(prefix):
                return True
            continue
        if fnmatch.fnmatchcase(normalized, pattern):
            return True
    return False


def classify_drift(
    *,
    previous_tree_sha: str | None,
    current_tree_sha: str,
    changed_paths: Sequence[str],
) -> DriftVerdict:
    """Classify one candidate's drift relative to the previously reviewed tree.

    Rules, in order:

    * ``previous_tree_sha`` is ``None`` -- there is no prior review on
      durable record to compare against -- always ``"semantic"``
      ("first review"). This is the fail-toward-semantic default: an
      unknown prior state is never treated as administrative-only.
    * the two tree SHAs are equal -- the tree is byte-identical (a
      rebase, or a commit-metadata-only change) -- always ``"none"``.
    * every changed path matches :func:`is_administrative_path` --
      ``"administrative"``. An EMPTY ``changed_paths`` with differing
      tree SHAs does NOT qualify (there is no positive evidence the
      difference is administrative-only) and falls through to semantic.
    * otherwise -- including any mix of administrative and non-
      administrative paths, and a lockfile-only change (lockfiles are
      never in :data:`ADMINISTRATIVE_PATH_PATTERNS`) -- ``"semantic"``.
    """

    paths = tuple(changed_paths)
    if previous_tree_sha is None:
        return DriftVerdict(kind="semantic", reason="first review", changed_paths=paths)
    if previous_tree_sha == current_tree_sha:
        return DriftVerdict(kind="none", reason="identical tree", changed_paths=paths)
    if paths and all(is_administrative_path(path) for path in paths):
        return DriftVerdict(
            kind="administrative",
            reason="only administrative paths changed",
            changed_paths=paths,
        )
    return DriftVerdict(
        kind="semantic", reason="semantic paths changed", changed_paths=paths
    )
