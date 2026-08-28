"""FileMutationJournal's write-ahead claim must survive a system crash.

The durable-write sequence for a claim is: fsync the temp journal file,
os.replace it atomically onto the journal path, then fsync an fd opened on
the journal's parent directory — all before the associated mutation
command's argv reaches the runner. Without the trailing directory fsync,
the rename itself is not guaranteed durable across a crash, even though the
file content was fsynced and the replace succeeded.
"""

from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from hermes_orchestrator.deploy.lifecycle import (
    CommandStep,
    FileMutationJournal,
    RunResult,
    execute_plan,
)

Event = tuple[str, object]


class _RecordingRunner:
    """Minimal runner that records every argv it is asked to run."""

    def __init__(self, events: list[Event]) -> None:
        self._events = events

    def run(self, argv, *, timeout: float) -> RunResult:
        self._events.append(("run", tuple(argv)))
        return RunResult(returncode=0, stdout="", stderr="")


def _install_recording_wrappers(
    monkeypatch: pytest.MonkeyPatch, events: list[Event]
) -> None:
    """Wrap os.fsync/os.replace to record classified events, then call through."""
    real_fsync = os.fsync
    real_replace = os.replace

    def fake_fsync(fd: int) -> None:
        # Classify before calling through: fstat still works on a valid fd
        # regardless of what the real fsync call below does to it.
        mode = os.fstat(fd).st_mode
        kind = "dir" if stat.S_ISDIR(mode) else "file"
        events.append((f"fsync-{kind}", fd))
        real_fsync(fd)

    def fake_replace(src, dst) -> None:
        real_replace(src, dst)
        events.append(("replace", (str(src), str(dst))))

    monkeypatch.setattr(os, "fsync", fake_fsync)
    monkeypatch.setattr(os, "replace", fake_replace)


def test_claim_write_fsyncs_file_replaces_then_fsyncs_parent_dir_before_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[Event] = []
    _install_recording_wrappers(monkeypatch, events)

    journal_path = tmp_path / "install-journal.json"
    journal = FileMutationJournal(journal_path)
    runner = _RecordingRunner(events)

    probe_argv = ("test", "-e", "/opt/hermes/rendered/thing.plist")
    mutate_argv = ("launchctl", "bootstrap", "gui/501", "/opt/hermes/thing.plist")
    steps = (
        CommandStep(argv=probe_argv, kind="probe", code="probe_thing"),
        CommandStep(
            argv=mutate_argv,
            kind="mutate",
            code="install_thing",
            resource="com.josystem.thing",
        ),
    )

    report = execute_plan(steps, runner, console_port=8787, journal=journal)

    assert report.completed

    run_indices = [
        index for index, event in enumerate(events) if event[0] == "run"
    ]
    mutate_run_index = next(
        index
        for index in run_indices
        if events[index][1] == mutate_argv
    )

    preceding = events[mutate_run_index - 3 : mutate_run_index]
    kinds = [event[0] for event in preceding]
    assert kinds == ["fsync-file", "replace", "fsync-dir"]

    temp_path = str(journal_path.with_name(journal_path.name + ".tmp"))
    replace_event = preceding[1]
    assert replace_event == ("replace", (temp_path, str(journal_path)))

    # Every replace onto the journal path must be followed by a
    # parent-directory fsync before any later "run" event -- not just the
    # one immediately preceding the mutate command.
    replace_indices = [
        index
        for index, event in enumerate(events)
        if event[0] == "replace" and event[1][1] == str(journal_path)
    ]
    assert replace_indices, "expected at least one replace onto the journal path"
    for replace_index in replace_indices:
        following = events[replace_index + 1 :]
        next_run_offset = next(
            (i for i, event in enumerate(following) if event[0] == "run"),
            len(following),
        )
        before_next_run = following[:next_run_offset]
        assert any(event[0] == "fsync-dir" for event in before_next_run), (
            "expected a parent-directory fsync after replace "
            f"(index {replace_index}) before the next run event"
        )
