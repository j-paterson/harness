"""ANSI scroll-region writer for the single Orchestrator surface."""

from __future__ import annotations

import io

from hermes_orchestrator.dashboard_pane import DashboardPane

_LINES = ("alpha", "beta", "gamma")


class _TtyStream(io.StringIO):
    def isatty(self) -> bool:  # pragma: no cover - trivial
        return True


class _BrokenIsattyStream(io.StringIO):
    def isatty(self) -> bool:
        raise ValueError("stream has no terminal identity")


def _pane(stream: io.StringIO, rows: int = 40) -> DashboardPane:
    return DashboardPane(stream, rows=lambda: rows)


def test_non_tty_stream_makes_every_operation_a_no_op() -> None:
    stream = io.StringIO()
    pane = _pane(stream)
    pane.draw(_LINES)
    pane.restore()
    assert stream.getvalue() == ""


def test_broken_isatty_fails_closed_to_no_op() -> None:
    stream = _BrokenIsattyStream()
    pane = _pane(stream)
    pane.draw(_LINES)
    pane.restore()
    assert stream.getvalue() == ""


def test_draw_establishes_region_and_redraws_in_place() -> None:
    stream = _TtyStream()
    pane = _pane(stream, rows=40)
    pane.draw(_LINES)
    output = stream.getvalue()

    # DECSTBM confines normal scrolling to the lower region and the
    # cursor is parked in the lower region for daemon stdout.
    assert "\x1b[4;40r" in output
    assert "\x1b[40;1H" in output
    # The dashboard block is redrawn under cursor save/restore with
    # explicit per-line positioning and clear-to-end-of-line.
    assert "\x1b7" in output
    assert "\x1b8" in output
    for index, line in enumerate(_LINES):
        assert f"\x1b[{index + 1};1H{line}\x1b[K" in output


def test_region_establishment_is_idempotent_across_draws() -> None:
    stream = _TtyStream()
    pane = _pane(stream, rows=40)
    pane.draw(_LINES)
    pane.draw(_LINES)
    assert stream.getvalue().count("\x1b[4;40r") == 1


def test_fresh_writer_reestablishes_the_region() -> None:
    # Crash recovery: a fresh writer over the same stream re-emits the
    # deterministic establishment sequence.
    stream = _TtyStream()
    _pane(stream, rows=40).draw(_LINES)
    _pane(stream, rows=40).draw(_LINES)
    assert stream.getvalue().count("\x1b[4;40r") == 2


def test_restore_resets_region_and_next_draw_reestablishes() -> None:
    stream = _TtyStream()
    pane = _pane(stream, rows=40)
    pane.draw(_LINES)
    pane.restore()
    assert "\x1b[r" in stream.getvalue()

    pane.draw(_LINES)
    assert stream.getvalue().count("\x1b[4;40r") == 2


def test_changed_line_count_moves_the_region_boundary() -> None:
    stream = _TtyStream()
    pane = _pane(stream, rows=40)
    pane.draw(_LINES)
    pane.draw((*_LINES, "delta"))
    output = stream.getvalue()
    assert "\x1b[4;40r" in output
    assert "\x1b[5;40r" in output
