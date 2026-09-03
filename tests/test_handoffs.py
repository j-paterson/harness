from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from uuid import UUID

import pytest

from hermes_orchestrator.db import Database
from hermes_orchestrator.handoffs import (
    HandoffDocument,
    HandoffRejected,
    HandoffService,
    HandoffStale,
    HandoffTest,
    refresh_handoff_head,
    require_current_head,
)


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Database]:
    value = Database.open(tmp_path / "state.db")
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def handoffs(database: Database) -> HandoffService:
    return HandoffService(database, handoff_ids=lambda: "handoff-1")


def valid_handoff() -> HandoffDocument:
    return HandoffDocument(
        cell_id="cell-demo",
        objective="Complete ENG-9 without expanding its scope.",
        status="Implementation is ready for the remaining unit-test correction.",
        decisions=["Keep the existing public interface."],
        branch="feature/eng-9",
        commits=["abc123 feat: implement ENG-9"],
        pull_request="https://github.example/pull/9",
        modified_files=["src/example.py", "tests/test_example.py"],
        tests=[HandoffTest(command="uv run pytest", outcome="1 failed, 12 passed")],
        blockers=[],
        remaining_steps=["Correct the failing boundary case."],
        commands=["uv run pytest tests/test_example.py -q"],
        environment_notes=["Python 3.13 through uv."],
        risks=[],
        next_action="Run the failing test and correct ENG-9.",
    )


def test_handoff_requires_every_contract_field(handoffs: HandoffService) -> None:
    incomplete = valid_handoff().model_copy(update={"tests": []})

    with pytest.raises(HandoffRejected, match="tests"):
        handoffs.submit(incomplete)


def test_handoff_persists_rendered_snapshot(handoffs: HandoffService) -> None:
    record = handoffs.submit(valid_handoff())

    restored = handoffs.get(record.handoff_id)

    assert restored.document == valid_handoff()
    assert "# Project lead handoff" in restored.markdown
    assert "uv run pytest" in restored.markdown
    assert restored.state == "submitted"


def test_acknowledgement_requires_restarted_next_action(
    handoffs: HandoffService,
) -> None:
    record = handoffs.submit(valid_handoff())

    with pytest.raises(HandoffRejected, match="next action"):
        handoffs.acknowledge(
            record.handoff_id,
            UUID("22222222-2222-4222-8222-222222222222"),
            "   ",
            profile_alias="max-b",
        )


REPLACEMENT = UUID("22222222-2222-4222-8222-222222222222")
NEXT_ACTION = "Run the failing test and correct ENG-9."


def test_acknowledgement_persists_replacement_profile_atomically(
    handoffs: HandoffService, database: Database
) -> None:
    """Sol b4b545f3 P2: the selected replacement session AND profile become
    durable in the same transition that marks the handoff acknowledged, so
    recovery can reconstruct the exact identities."""

    record = handoffs.submit(valid_handoff())

    acknowledged = handoffs.acknowledge(
        record.handoff_id, REPLACEMENT, NEXT_ACTION, profile_alias="max-b"
    )

    assert acknowledged.state == "acknowledged"
    assert acknowledged.replacement_session_id == REPLACEMENT
    assert acknowledged.replacement_profile_alias == "max-b"
    row = database.execute(
        "SELECT state, replacement_session_id, replacement_profile_alias "
        "FROM handoffs WHERE handoff_id = ?",
        (record.handoff_id,),
    ).fetchone()
    assert str(row["state"]) == "acknowledged"
    assert str(row["replacement_session_id"]) == str(REPLACEMENT)
    assert str(row["replacement_profile_alias"]) == "max-b"


def test_acknowledgement_requires_replacement_profile(
    handoffs: HandoffService, database: Database
) -> None:
    record = handoffs.submit(valid_handoff())

    with pytest.raises(HandoffRejected, match="profile"):
        handoffs.acknowledge(
            record.handoff_id, REPLACEMENT, NEXT_ACTION, profile_alias="   "
        )

    row = database.execute(
        "SELECT state, replacement_session_id, replacement_profile_alias "
        "FROM handoffs WHERE handoff_id = ?",
        (record.handoff_id,),
    ).fetchone()
    assert str(row["state"]) == "submitted"
    assert row["replacement_session_id"] is None
    assert row["replacement_profile_alias"] is None


def test_repeat_acknowledgement_with_same_identities_is_idempotent(
    handoffs: HandoffService,
) -> None:
    record = handoffs.submit(valid_handoff())
    handoffs.acknowledge(
        record.handoff_id, REPLACEMENT, NEXT_ACTION, profile_alias="max-b"
    )

    repeated = handoffs.acknowledge(
        record.handoff_id, REPLACEMENT, NEXT_ACTION, profile_alias="max-b"
    )

    assert repeated.state == "acknowledged"
    assert repeated.replacement_profile_alias == "max-b"


def test_repeat_acknowledgement_with_a_different_profile_is_rejected(
    handoffs: HandoffService,
) -> None:
    record = handoffs.submit(valid_handoff())
    handoffs.acknowledge(
        record.handoff_id, REPLACEMENT, NEXT_ACTION, profile_alias="max-b"
    )

    with pytest.raises(HandoffRejected, match="profile"):
        handoffs.acknowledge(
            record.handoff_id, REPLACEMENT, NEXT_ACTION, profile_alias="max-c"
        )


def test_reacknowledgement_backfills_a_legacy_row_without_a_profile(
    handoffs: HandoffService, database: Database
) -> None:
    """A row acknowledged before migration 0051 carries NULL for the
    replacement profile; an identical re-acknowledgement that now names the
    profile repairs the row instead of rejecting it."""

    record = handoffs.submit(valid_handoff())
    with database.transaction() as connection:
        connection.execute(
            "UPDATE handoffs SET state = 'acknowledged', "
            "replacement_session_id = ?, restated_next_action = ? "
            "WHERE handoff_id = ?",
            (str(REPLACEMENT), NEXT_ACTION, record.handoff_id),
        )

    repaired = handoffs.acknowledge(
        record.handoff_id, REPLACEMENT, NEXT_ACTION, profile_alias="max-b"
    )

    assert repaired.state == "acknowledged"
    assert repaired.replacement_profile_alias == "max-b"


# -- post-commit submission signal and derived documents (INFRA-198) -------


def test_submit_signals_subscribers_after_the_durable_commit(
    handoffs: HandoffService, database: Database
) -> None:
    observed: list[tuple[str, int]] = []

    def listener(record: object) -> None:
        # The durable row exists by the time the signal runs.
        count = database.scalar(
            "SELECT count(*) FROM handoffs WHERE handoff_id = ?",
            (record.handoff_id,),  # type: ignore[attr-defined]
        )
        observed.append((record.handoff_id, int(count)))  # type: ignore[attr-defined]

    handoffs.subscribe(listener)

    record = handoffs.submit(valid_handoff())

    assert observed == [(record.handoff_id, 1)]


def test_a_failing_listener_never_unsubmits_the_durable_handoff(
    handoffs: HandoffService, database: Database
) -> None:
    def broken(record: object) -> None:
        raise RuntimeError("listener crashed")

    handoffs.subscribe(broken)

    record = handoffs.submit(valid_handoff())

    assert record.state == "submitted"
    assert database.scalar("SELECT count(*) FROM handoffs") == 1


def test_derived_handoff_document_takes_only_non_derivable_content() -> None:
    """Every mechanical field comes from durable facts; the incumbent
    contributes only decisions, caveats, risks, and the next action."""

    from hermes_orchestrator.handoffs import derived_handoff_document

    document = derived_handoff_document(
        cell_id="cell-demo",
        project_key="demo",
        session_id="11111111-1111-4111-8111-111111111111",
        profile_alias="max-b",
        issue_id="ENG-9",
        issue_state="in_development",
        branch="feature/eng-9",
        head="abc123",
        decisions=["Keep the existing public interface."],
        caveats=["CI flake on the network suite."],
        risks=["Schema migration is irreversible."],
        next_action="Run the failing test and correct ENG-9.",
    )

    assert document.cell_id == "cell-demo"
    assert document.branch == "feature/eng-9"
    assert document.commits == ["abc123"]
    assert "ENG-9" in document.objective
    assert "in_development" in document.status
    assert document.decisions == ["Keep the existing public interface."]
    assert document.blockers == ["CI flake on the network suite."]
    assert document.risks == ["Schema migration is irreversible."]
    assert document.next_action == "Run the failing test and correct ENG-9."
    assert document.remaining_steps == [document.next_action]
    assert any("max-b" in note for note in document.environment_notes)


def test_derived_document_from_an_unreadable_worktree_fails_submission(
    handoffs: HandoffService,
) -> None:
    """A worktree probe that could not resolve the branch/head yields a
    document whose mechanical fields fail submit()'s revalidation — an
    unverifiable checkpoint is never stored as a handoff."""

    from hermes_orchestrator.handoffs import derived_handoff_document

    document = derived_handoff_document(
        cell_id="cell-demo",
        project_key="demo",
        session_id="11111111-1111-4111-8111-111111111111",
        profile_alias="max-b",
        issue_id="ENG-9",
        issue_state="in_development",
        branch="",
        head="",
        decisions=["Keep the existing public interface."],
        caveats=[],
        risks=[],
        next_action="Run the failing test and correct ENG-9.",
    )

    with pytest.raises(HandoffRejected, match=r"branch|commits"):
        handoffs.submit(document)


def test_derived_handoff_document_reports_real_pr_files_tests_and_corrections() -> None:
    """INFRA-184: the pull request, the files actually changed, observed
    test/acceptance evidence, and pending Merger corrections all come
    from the caller's ``DerivedFacts`` -- not a synthetic one-line
    placeholder -- and land in the document's existing sections."""

    from hermes_orchestrator.handoffs import DerivedFacts, derived_handoff_document

    document = derived_handoff_document(
        cell_id="cell-demo",
        project_key="demo",
        session_id="11111111-1111-4111-8111-111111111111",
        profile_alias="max-b",
        issue_id="ENG-9",
        issue_state="in_development",
        branch="feature/eng-9",
        head="abc123",
        decisions=["Keep the existing public interface."],
        caveats=["CI flake on the network suite."],
        risks=["Schema migration is irreversible."],
        next_action="Run the failing test and correct ENG-9.",
        facts=DerivedFacts(
            pull_request="#91 open https://github.com/owner/demo/pull/91",
            modified_files=("src/example.py", "tests/test_example.py"),
            test_results=(
                "subagent packet packet-1 — accepted: diff=scoped",
                "acceptance gate instr-1 — satisfied: predicate=covered",
            ),
            pending_corrections=("correction-1 (codex, pr #91)",),
        ),
    )

    assert document.pull_request == "#91 open https://github.com/owner/demo/pull/91"
    assert document.modified_files == ["src/example.py", "tests/test_example.py"]
    assert [test.command for test in document.tests] == [
        "subagent packet packet-1",
        "acceptance gate instr-1",
    ]
    assert [test.outcome for test in document.tests] == [
        "accepted: diff=scoped",
        "satisfied: predicate=covered",
    ]
    assert any(
        "correction-1 (codex, pr #91)" in note for note in document.environment_notes
    )
    # The incumbent's own inputs are untouched by the derived facts.
    assert document.decisions == ["Keep the existing public interface."]
    assert document.blockers == ["CI flake on the network suite."]
    assert document.risks == ["Schema migration is irreversible."]


def test_derived_handoff_document_falls_back_when_no_facts_are_recorded(
    handoffs: HandoffService,
) -> None:
    """No durable PR/test/correction evidence exists yet: the document
    still satisfies every non-empty field with an honest placeholder,
    not silence, and submits cleanly."""

    from hermes_orchestrator.handoffs import derived_handoff_document

    document = derived_handoff_document(
        cell_id="cell-demo",
        project_key="demo",
        session_id="11111111-1111-4111-8111-111111111111",
        profile_alias="max-b",
        issue_id="ENG-9",
        issue_state="in_development",
        branch="feature/eng-9",
        head="abc123",
        decisions=["Keep the existing public interface."],
        caveats=[],
        risks=[],
        next_action="Run the failing test and correct ENG-9.",
    )

    assert document.pull_request == "none recorded in durable state"
    assert document.modified_files
    assert document.tests[0].outcome == "none recorded in durable state"
    assert any(
        "no pending Merger corrections" in note for note in document.environment_notes
    )
    handoffs.submit(document)


def test_validate_handoff_identity_refuses_mismatched_cell_or_session() -> None:
    """INFRA-184: identity is validated mechanically before submission.
    A document naming a different cell than the live submitting seat,
    and a live cell whose session no longer matches who is submitting,
    both refuse -- fail closed, no write."""

    from hermes_orchestrator.handoffs import LiveCell, validate_handoff_identity

    document = valid_handoff().model_copy(update={"cell_id": "cell-demo"})

    # cell_id mismatch: the document names a cell other than the live
    # submitting seat.
    with pytest.raises(HandoffRejected, match="cell"):
        validate_handoff_identity(
            document,
            LiveCell(cell_id="cell-other", session_id="session-1"),
            requesting_session_id="session-1",
        )

    # session mismatch: the live cell has been reassigned to a
    # different session since the document was derived.
    with pytest.raises(HandoffRejected, match="session"):
        validate_handoff_identity(
            document,
            LiveCell(cell_id="cell-demo", session_id="session-2"),
            requesting_session_id="session-1",
        )

    # a live cell with no session at all (never seated) also refuses.
    with pytest.raises(HandoffRejected, match="session"):
        validate_handoff_identity(
            document,
            LiveCell(cell_id="cell-demo", session_id=None),
            requesting_session_id="session-1",
        )

    # matching identity does not raise.
    validate_handoff_identity(
        document,
        LiveCell(cell_id="cell-demo", session_id="session-1"),
        requesting_session_id="session-1",
    )


# -- currency verification and mechanical refresh (INFRA-222) --------------


def test_handoff_stale_is_a_handoff_rejected() -> None:
    """Every existing ``except HandoffRejected`` catch must still catch
    a staleness refusal."""

    assert issubclass(HandoffStale, HandoffRejected)


def test_require_current_head_accepts_a_matching_branch_and_head() -> None:
    document = valid_handoff()

    require_current_head(
        document,
        branch="feature/eng-9",
        head="abc123 feat: implement ENG-9",
    )


def test_require_current_head_raises_on_a_branch_mismatch() -> None:
    document = valid_handoff()

    with pytest.raises(HandoffStale, match="branch"):
        require_current_head(
            document,
            branch="feature/eng-9-renamed",
            head="abc123 feat: implement ENG-9",
        )


def test_require_current_head_raises_on_a_stale_head() -> None:
    """The branch still matches, but the leased worktree's HEAD has
    advanced past what this document recorded."""

    document = valid_handoff()

    with pytest.raises(HandoffStale, match="head"):
        require_current_head(
            document,
            branch="feature/eng-9",
            head="def456 later commit",
        )


def test_refresh_handoff_head_updates_branch_and_notes_the_previous_head() -> None:
    document = valid_handoff()

    refreshed = refresh_handoff_head(
        document, branch="feature/eng-9", head="def456 later commit"
    )

    assert refreshed.branch == "feature/eng-9"
    assert refreshed.commits == ["def456 later commit"]
    assert any(
        "abc123 feat: implement ENG-9" in note and "def456 later commit" in note
        for note in refreshed.environment_notes
    )
    # every other field survives untouched
    assert refreshed.cell_id == document.cell_id
    assert refreshed.next_action == document.next_action
    # the original document is never mutated
    assert document.commits == ["abc123 feat: implement ENG-9"]

    # the refreshed document is now current.
    require_current_head(refreshed, branch="feature/eng-9", head="def456 later commit")


def test_handoff_service_refresh_persists_the_new_document_in_place(
    handoffs: HandoffService, database: Database
) -> None:
    record = handoffs.submit(valid_handoff())

    refreshed_document = refresh_handoff_head(
        record.document, branch="feature/eng-9", head="def456 later commit"
    )

    refreshed = handoffs.refresh(record.handoff_id, refreshed_document)

    assert refreshed.handoff_id == record.handoff_id
    assert refreshed.state == "submitted"
    assert refreshed.document.commits == ["def456 later commit"]
    assert "def456 later commit" in refreshed.markdown
    row = database.execute(
        "SELECT document_json, markdown FROM handoffs WHERE handoff_id = ?",
        (record.handoff_id,),
    ).fetchone()
    assert "def456 later commit" in str(row["markdown"])
    assert "def456 later commit" in str(row["document_json"])


def test_handoff_service_refresh_preserves_acknowledgement_state(
    handoffs: HandoffService,
) -> None:
    record = handoffs.submit(valid_handoff())
    handoffs.acknowledge(
        record.handoff_id, REPLACEMENT, NEXT_ACTION, profile_alias="max-b"
    )

    refreshed_document = refresh_handoff_head(
        record.document, branch="feature/eng-9", head="def456 later commit"
    )
    refreshed = handoffs.refresh(record.handoff_id, refreshed_document)

    assert refreshed.state == "acknowledged"
    assert refreshed.replacement_session_id == REPLACEMENT
    assert refreshed.replacement_profile_alias == "max-b"
    assert refreshed.document.commits == ["def456 later commit"]


def test_handoff_service_refresh_refuses_a_document_naming_a_different_cell(
    handoffs: HandoffService,
) -> None:
    record = handoffs.submit(valid_handoff())

    mismatched = valid_handoff().model_copy(update={"cell_id": "cell-other"})

    with pytest.raises(HandoffRejected, match="cell"):
        handoffs.refresh(record.handoff_id, mismatched)

    # nothing changed on the durable row.
    assert handoffs.get(record.handoff_id).document == valid_handoff()
