"""Deterministic GitHub pull-request reads and compare-and-merge mutation."""

from __future__ import annotations

import json
import re
import sqlite3
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

import httpx

from hermes_orchestrator.db import Database

_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_REF_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]*$")
_REPOSITORY_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9][A-Za-z0-9._-]*$"
)
_MERGE_METHODS = frozenset({"merge", "squash", "rebase"})
_API_VERSION = "2022-11-28"


class GitHubError(RuntimeError):
    """Raised when GitHub state cannot be established deterministically."""


class MergeBlocked(GitHubError):
    """Raised before any mutation when the merge preconditions fail."""


class MergeRaceLost(MergeBlocked):
    """Raised when GitHub rejected the exact expected head at merge time."""


class MergeAmbiguous(GitHubError):
    """Merge ownership cannot be proven either way; reconciliation required.

    Raised when the pull request is already merged with exactly the expected
    identity while this journal holds only a pending intent: the intent alone
    is never proof the mutation was ours, so nothing may be adopted.
    """


class MergeInFlight(GitHubError):
    """Another live attempt holds the execution lease for this effect."""


@dataclass(frozen=True, slots=True)
class GitHubResponse:
    """One transport-level GitHub response."""

    status: int
    payload: Any


class GitHubTransport(Protocol):
    """Narrow injected REST boundary; the client never builds URLs elsewhere."""

    def request(
        self,
        method: str,
        path: str,
        *,
        query: Mapping[str, str] | None = None,
        body: Mapping[str, Any] | None = None,
    ) -> GitHubResponse: ...


class HttpxGitHubTransport:
    """HTTPS transport for api.github.com."""

    def __init__(
        self,
        token: str,
        *,
        client: httpx.Client | None = None,
    ) -> None:
        if not token:
            raise ValueError("GitHub token is required")
        self._token = token
        self._client = client

    def request(
        self,
        method: str,
        path: str,
        *,
        query: Mapping[str, str] | None = None,
        body: Mapping[str, Any] | None = None,
    ) -> GitHubResponse:
        owns_client = self._client is None
        client = self._client or httpx.Client(base_url="https://api.github.com")
        try:
            response = client.request(
                method,
                path,
                params=dict(query) if query is not None else None,
                json=body,
                headers={
                    "Accept": "application/vnd.github+json",
                    "Authorization": f"Bearer {self._token}",
                    "X-GitHub-Api-Version": _API_VERSION,
                },
            )
        finally:
            if owns_client:
                client.close()
        try:
            payload = response.json()
        except ValueError:
            payload = None
        return GitHubResponse(response.status_code, payload)


@dataclass(frozen=True, slots=True)
class PullRequest:
    """The validated full pull-request identity every merge decision binds."""

    repository: str
    number: int
    state: str
    draft: bool
    merged: bool
    mergeable: bool | None
    merge_commit_sha: str | None
    head_sha: str
    head_ref: str
    head_repository: str
    base_ref: str
    base_repository: str


@dataclass(frozen=True, slots=True)
class PullRequestSummary:
    """One strict pull-request list entry.

    GitHub's list schema (pull-request-simple) omits the ``merged`` and
    ``mergeable`` decision fields, so a summary can never authorize a merge
    decision; eligibility always requires a fresh full read.
    """

    repository: str
    number: int
    state: str
    draft: bool
    head_sha: str
    head_ref: str
    head_repository: str
    base_ref: str
    base_repository: str


@dataclass(frozen=True, slots=True)
class MergeResult:
    """The confirmed outcome of one deterministic merge mutation."""

    merge_sha: str
    already_merged: bool


@dataclass(frozen=True, slots=True)
class MergeEffect:
    """One durable merge-mutation journal entry."""

    effect_id: str
    state: str
    request: dict[str, Any]
    response: dict[str, Any] | None
    claim_token: str | None
    claim_expires_at: str | None


@dataclass(frozen=True, slots=True)
class MergeClaim:
    """One granted execution claim, or the completed response to replay.

    ``token`` is set when this caller owns the single live mutation attempt;
    ``completed_response`` is set instead when the effect already completed
    and must be replayed without a mutation.
    """

    effect_id: str
    token: str | None
    completed_response: dict[str, Any] | None


_REQUEST_KEYS = frozenset(
    {"repository", "number", "sha", "head_ref", "base", "merge_method"}
)


class MergeEffectJournal:
    """Durable journal for GitHub merge mutations in a dedicated table.

    Three invariants hold across retries, restarts, and concurrent
    connections: one effect id is bound forever to one complete immutable
    request; one canonical merge candidate — repository, pull request,
    exact head SHA — has at most one owning effect; and one live execution
    claim exists per effect, so at most one caller may send the mutation.
    All are enforced by the database, not by callers.
    """

    def __init__(
        self,
        database: Database,
        *,
        now: Callable[[], datetime] | None = None,
        lease_seconds: float = 300.0,
    ) -> None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self._database = database
        self._now = now if now is not None else lambda: datetime.now(UTC)
        self._lease_seconds = lease_seconds

    def claim(self, effect_id: str, *, request: dict[str, Any]) -> MergeClaim:
        """Atomically take the single execution claim for one merge effect.

        Exactly one caller receives a token per lease window; a competitor
        fails closed as in flight without any GitHub call. An expired lease
        may be reclaimed once (crash/restart recovery), and a completed
        effect is returned for replay instead of a token.
        """

        if set(request) != _REQUEST_KEYS:
            raise GitHubError("merge effect request is missing binding fields")
        now = self._now()
        stamp = now.isoformat()
        expires = (now + timedelta(seconds=self._lease_seconds)).isoformat()
        token = uuid.uuid4().hex
        existing = self.get(effect_id)
        if existing is not None:
            return self._claim_existing(existing, request, token, stamp, expires)
        request_json = json.dumps(request, sort_keys=True, separators=(",", ":"))
        try:
            with self._database.transaction() as connection:
                connection.execute(
                    "INSERT INTO github_merge_effects("
                    "effect_id, repository, pr_number, head_sha, head_ref, "
                    "base_ref, merge_method, state, request_json, "
                    "response_json, claim_token, claim_expires_at, "
                    "created_at, updated_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, NULL, ?, ?, "
                    "?, ?)",
                    (
                        effect_id,
                        request["repository"],
                        request["number"],
                        request["sha"],
                        request["head_ref"],
                        request["base"],
                        request["merge_method"],
                        request_json,
                        token,
                        expires,
                        stamp,
                        stamp,
                    ),
                )
        except sqlite3.IntegrityError as error:
            racing = self.get(effect_id)
            if racing is None:
                raise GitHubError(
                    "merge candidate is already owned by another effect"
                ) from error
            return self._claim_existing(racing, request, token, stamp, expires)
        return MergeClaim(effect_id, token, None)

    def _claim_existing(
        self,
        existing: MergeEffect,
        request: dict[str, Any],
        token: str,
        stamp: str,
        expires: str,
    ) -> MergeClaim:
        if existing.request != request:
            raise GitHubError(
                "effect id was already used for a different merge request"
            )
        if existing.state == "completed" and existing.response is not None:
            return MergeClaim(existing.effect_id, None, existing.response)
        if existing.state == "attempted":
            raise MergeAmbiguous(
                "reconciliation required: a prior attempt crossed the "
                "mutation boundary with an unknown outcome"
            )
        # Only a pre-send pending claim may be reclaimed, and only after
        # its lease expired; a claim that crossed the mutation boundary is
        # in state 'attempted' and excluded here forever.
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE github_merge_effects SET claim_token = ?, "
                "claim_expires_at = ?, updated_at = ? "
                "WHERE effect_id = ? AND state = 'pending' "
                "AND (claim_token IS NULL OR claim_expires_at IS NULL "
                "OR claim_expires_at <= ?)",
                (token, expires, stamp, existing.effect_id, stamp),
            )
        if cursor.rowcount == 1:
            return MergeClaim(existing.effect_id, token, None)
        refreshed = self.get(existing.effect_id)
        if (
            refreshed is not None
            and refreshed.state == "completed"
            and refreshed.response is not None
        ):
            return MergeClaim(existing.effect_id, None, refreshed.response)
        if refreshed is not None and refreshed.state == "attempted":
            raise MergeAmbiguous(
                "reconciliation required: a prior attempt crossed the "
                "mutation boundary with an unknown outcome"
            )
        raise MergeInFlight(
            "merge mutation for this effect is already in flight"
        )

    def mark_attempted(self, effect_id: str, *, token: str | None) -> None:
        """Fence the exact live claim at the external mutation boundary.

        From this durable transition on, no lease expiry or restart can
        grant another execution claim; only this exact token may complete
        or release the row.
        """

        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE github_merge_effects SET state = 'attempted', "
                "updated_at = ? "
                "WHERE effect_id = ? AND state = 'pending' "
                "AND claim_token = ?",
                (self._now().isoformat(), effect_id, token),
            )
        if cursor.rowcount != 1:
            raise GitHubError(
                "stale claim token cannot reach the mutation boundary"
            )

    def complete(
        self, effect_id: str, response: dict[str, Any], *, token: str | None
    ) -> MergeEffect:
        """Record the confirmed merge under the exact attempted token.

        Completion is valid only from the 'attempted' boundary state: a
        claim that never reached the mutation boundary, or whose token was
        superseded, cannot complete.
        """

        response_json = json.dumps(response, sort_keys=True, separators=(",", ":"))
        with self._database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE github_merge_effects SET state = 'completed', "
                "response_json = ?, claim_token = NULL, "
                "claim_expires_at = NULL, updated_at = ? "
                "WHERE effect_id = ? AND state = 'attempted' "
                "AND claim_token = ?",
                (response_json, self._now().isoformat(), effect_id, token),
            )
        if cursor.rowcount != 1:
            raise GitHubError(
                "stale claim token cannot complete the merge effect"
            )
        completed = self.get(effect_id)
        if completed is None:
            raise GitHubError("completed merge effect disappeared")
        return completed

    def release(self, effect_id: str, *, token: str | None) -> None:
        """Reset an attempted mutation that definitively did not merge.

        Callable only with the exact attempted token and only when the
        response proved no mutation happened; the row returns to a
        reclaimable pre-send pending state. A stale token is a no-op so it
        can never disturb a newer owner.
        """

        with self._database.transaction() as connection:
            connection.execute(
                "UPDATE github_merge_effects SET state = 'pending', "
                "claim_token = NULL, claim_expires_at = NULL, updated_at = ? "
                "WHERE effect_id = ? AND state = 'attempted' "
                "AND claim_token = ?",
                (self._now().isoformat(), effect_id, token),
            )

    def get(self, effect_id: str) -> MergeEffect | None:
        """Read one merge effect from the durable journal."""

        row = self._database.execute(
            "SELECT effect_id, state, request_json, response_json, "
            "claim_token, claim_expires_at "
            "FROM github_merge_effects WHERE effect_id = ?",
            (effect_id,),
        ).fetchone()
        if row is None:
            return None
        return MergeEffect(
            effect_id=str(row["effect_id"]),
            state=str(row["state"]),
            request=json.loads(row["request_json"]),
            response=(
                json.loads(row["response_json"])
                if row["response_json"] is not None
                else None
            ),
            claim_token=(
                str(row["claim_token"])
                if row["claim_token"] is not None
                else None
            ),
            claim_expires_at=(
                str(row["claim_expires_at"])
                if row["claim_expires_at"] is not None
                else None
            ),
        )


class GitHubClient:
    """Read pull requests and merge exactly one expected head, fail closed."""

    def __init__(
        self,
        *,
        transport: GitHubTransport,
        journal: MergeEffectJournal,
    ) -> None:
        self._transport = transport
        self._journal = journal

    def get_pull_request(self, repository: str, number: int) -> PullRequest:
        """Read one pull request and validate its full identity."""

        _require_repository(repository)
        _require_number(number)
        response = self._transport.request(
            "GET", f"/repos/{repository}/pulls/{number}"
        )
        if response.status != 200:
            raise GitHubError(
                f"GitHub pull-request read returned status {response.status}"
            )
        pull = _parse_pull_request(repository, response.payload)
        if pull.number != number:
            raise GitHubError(
                "stale GitHub response: payload is for a different pull request"
            )
        return pull

    def list_open_pulls(
        self, repository: str, *, base: str
    ) -> tuple[PullRequestSummary, ...]:
        """List every open pull request targeting one integration branch.

        Returns strict summaries only; the list schema carries no merge
        decision fields, so callers must re-read any relevant pull request
        through :meth:`get_pull_request` before acting on it.
        """

        _require_repository(repository)
        _require_ref(base, "base branch")
        response = self._transport.request(
            "GET",
            f"/repos/{repository}/pulls",
            query={"state": "open", "base": base, "per_page": "100"},
        )
        if response.status != 200:
            raise GitHubError(
                f"GitHub pull-request list returned status {response.status}"
            )
        if not isinstance(response.payload, list):
            raise GitHubError("GitHub pull-request list is not an array")
        summaries = tuple(
            _parse_pull_summary(repository, entry) for entry in response.payload
        )
        for summary in summaries:
            if summary.base_ref != base or summary.state != "open":
                raise GitHubError(
                    "stale GitHub response: listed pull does not match the filter"
                )
        return summaries

    def merge(
        self,
        repository: str,
        number: int,
        *,
        expected_head_sha: str,
        expected_head_ref: str,
        expected_base: str,
        effect_id: str,
        merge_method: str = "squash",
    ) -> MergeResult:
        """Merge exactly the expected head or fail closed without mutating.

        The pull request is re-read immediately before the mutation and every
        identity field must match. The complete immutable binding —
        repository, number, head SHA, head ref, base, and merge method — is
        journaled durably before the mutation is sent; a completed journal
        entry replays idempotently only for the identical binding, and the
        canonical candidate has exactly one owning effect across connections
        and restarts. An already-merged pull request is adopted only from a
        durably completed journal entry; a pending intent alone is ambiguous
        and fails closed.
        """

        _require_repository(repository)
        _require_number(number)
        _require_sha(expected_head_sha)
        _require_ref(expected_head_ref, "head branch")
        _require_ref(expected_base, "base branch")
        if not effect_id:
            raise GitHubError("merge requires a non-empty effect id")
        if merge_method not in _MERGE_METHODS:
            raise GitHubError(f"unknown merge method {merge_method!r}")
        request = {
            "repository": repository,
            "number": number,
            "sha": expected_head_sha,
            "head_ref": expected_head_ref,
            "base": expected_base,
            "merge_method": merge_method,
        }
        existing = self._journal.get(effect_id)
        if existing is not None and existing.request != request:
            raise GitHubError(
                "effect id was already used for a different merge request"
            )
        if (
            existing is not None
            and existing.state == "completed"
            and existing.response is not None
        ):
            return MergeResult(
                merge_sha=str(existing.response["sha"]), already_merged=True
            )

        pull = self.get_pull_request(repository, number)
        if pull.head_repository != repository or pull.base_repository != repository:
            raise MergeBlocked("pull request repository does not match the project")
        if pull.base_ref != expected_base:
            raise MergeBlocked(
                "pull request base branch is not the integration branch"
            )
        if pull.head_ref != expected_head_ref:
            raise MergeBlocked("pull request branch does not match the review")
        if pull.merged:
            # Full identity matched above except the head; check it before
            # deciding ownership so a foreign merge is named as foreign.
            if pull.head_sha != expected_head_sha:
                raise MergeBlocked(
                    "merged pull request head is not the reviewed head"
                )
            if existing is not None:
                raise MergeAmbiguous(
                    "reconciliation required: the pull request is merged but "
                    "this journal holds only a pending intent, which is not "
                    "proof of ownership"
                )
            raise MergeBlocked(
                "pull request was merged outside the deterministic adapter"
            )
        if pull.state != "open":
            raise MergeBlocked("pull request is not open")
        if pull.draft:
            raise MergeBlocked("pull request is a draft")
        if pull.head_sha != expected_head_sha:
            raise MergeBlocked("pull request head changed since the review")
        if pull.mergeable is not True:
            raise MergeBlocked("pull request is not cleanly mergeable")

        claim = self._journal.claim(effect_id, request=request)
        if claim.token is None:
            # Completed by a concurrent attempt between read and claim.
            return MergeResult(
                merge_sha=str(claim.completed_response["sha"]),
                already_merged=True,
            )
        # Durable fence at the external mutation boundary: from here on no
        # lease expiry or restart can grant another execution claim, and
        # only responses that definitively prove no merge may reset it.
        self._journal.mark_attempted(effect_id, token=claim.token)
        try:
            response = self._transport.request(
                "PUT",
                f"/repos/{repository}/pulls/{number}/merge",
                body={"sha": expected_head_sha, "merge_method": merge_method},
            )
        except Exception as error:
            # Unknown outcome: the row stays attempted/fenced; the next
            # reread decides between ambiguity (merged without completed
            # ownership) and operator reconciliation.
            raise GitHubError(
                "merge mutation outcome is unknown: transport failed"
            ) from error
        if response.status == 409:
            # Documented no-merge: GitHub rejected the expected head.
            self._journal.release(effect_id, token=claim.token)
            raise MergeRaceLost("pull request head changed at merge time")
        if response.status == 405:
            # Documented no-merge: the pull request was not mergeable.
            self._journal.release(effect_id, token=claim.token)
            raise MergeBlocked("GitHub refused the merge with status 405")
        if response.status != 200:
            # Unclassified outcome: stay attempted/fenced.
            raise GitHubError(f"GitHub merge returned status {response.status}")
        payload = response.payload
        if not isinstance(payload, dict) or not isinstance(
            payload.get("merged"), bool
        ):
            raise MergeAmbiguous(
                "reconciliation required: the merge response is malformed "
                "and may conceal a completed mutation"
            )
        if payload["merged"] is False:
            # Structurally valid proof that no merge was performed.
            self._journal.release(effect_id, token=claim.token)
            raise GitHubError(
                "GitHub confirmed no merge was performed; retry is safe"
            )
        merge_sha = payload.get("sha")
        if (
            not isinstance(merge_sha, str)
            or _SHA_PATTERN.match(merge_sha) is None
        ):
            raise MergeAmbiguous(
                "reconciliation required: GitHub confirmed a merge without "
                "a valid commit identity"
            )
        self._journal.complete(
            effect_id, {"merged": True, "sha": merge_sha}, token=claim.token
        )
        return MergeResult(merge_sha=merge_sha, already_merged=False)


def _parse_pull_request(repository: str, payload: Any) -> PullRequest:
    identity = _parse_common_identity(repository, payload)
    merged = payload.get("merged")
    if not isinstance(merged, bool):
        raise GitHubError("pull request merged flag is invalid")
    mergeable = payload.get("mergeable")
    if mergeable is not None and not isinstance(mergeable, bool):
        raise GitHubError("pull request mergeable flag is invalid")
    merge_commit_sha = payload.get("merge_commit_sha")
    if merge_commit_sha is not None and (
        not isinstance(merge_commit_sha, str)
        or _SHA_PATTERN.match(merge_commit_sha) is None
    ):
        raise GitHubError("pull request merge commit identity is invalid")
    return PullRequest(
        merged=merged,
        mergeable=mergeable,
        merge_commit_sha=merge_commit_sha,
        **identity,
    )


def _parse_pull_summary(repository: str, payload: Any) -> PullRequestSummary:
    return PullRequestSummary(**_parse_common_identity(repository, payload))


def _parse_common_identity(repository: str, payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise GitHubError("pull request payload is not an object")
    number = payload.get("number")
    if not isinstance(number, int) or isinstance(number, bool) or number < 1:
        raise GitHubError("pull request number is invalid")
    state = payload.get("state")
    if state not in ("open", "closed"):
        raise GitHubError(f"pull request state {state!r} is invalid")
    draft = payload.get("draft")
    if not isinstance(draft, bool):
        raise GitHubError("pull request draft flag is invalid")
    head = _require_side(payload, "head")
    base = _require_side(payload, "base")
    head_sha = payload["head"].get("sha")
    if not isinstance(head_sha, str) or _SHA_PATTERN.match(head_sha) is None:
        raise GitHubError("pull request head SHA is invalid")
    if base["repo"] != repository:
        raise GitHubError(
            "stale GitHub response: pull request belongs to another repository"
        )
    return {
        "repository": repository,
        "number": number,
        "state": state,
        "draft": draft,
        "head_sha": head_sha,
        "head_ref": head["ref"],
        "head_repository": head["repo"],
        "base_ref": base["ref"],
        "base_repository": base["repo"],
    }


def _require_side(payload: dict[str, Any], side: str) -> dict[str, str]:
    value = payload.get(side)
    if not isinstance(value, dict):
        raise GitHubError(f"pull request {side} is invalid")
    ref = value.get("ref")
    if not isinstance(ref, str) or _REF_PATTERN.match(ref) is None:
        raise GitHubError(f"pull request {side} ref is invalid")
    repo = value.get("repo")
    if not isinstance(repo, dict):
        raise GitHubError(f"pull request {side} repository is invalid")
    full_name = repo.get("full_name")
    if (
        not isinstance(full_name, str)
        or _REPOSITORY_PATTERN.match(full_name) is None
    ):
        raise GitHubError(f"pull request {side} repository is invalid")
    return {"ref": ref, "repo": full_name}


def _require_repository(value: str) -> None:
    if _REPOSITORY_PATTERN.match(value) is None:
        raise GitHubError("repository must be a safe owner/name identifier")


def _require_number(value: int) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise GitHubError("pull request number must be a positive integer")


def _require_sha(value: str) -> None:
    if not isinstance(value, str) or _SHA_PATTERN.match(value) is None:
        raise GitHubError("expected head must be a full lowercase hexadecimal SHA")


def _require_ref(value: str, label: str) -> None:
    if (
        not isinstance(value, str)
        or _REF_PATTERN.match(value) is None
        or ".." in value
    ):
        raise GitHubError(f"{label} contains unsafe characters")
