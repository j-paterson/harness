"""ANSI scroll-region writer for the single Orchestrator surface."""

from __future__ import annotations

import io

from hermes_orchestrator.dashboard_pane import DashboardPane, FramePane

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


# ---------------------------------------------------------------------------
# INFRA-209: FramePane — a full-frame in-place redraw with no-flicker
# diffing (only changed rows are rewritten between same-height draws).
# ---------------------------------------------------------------------------


def test_frame_pane_non_tty_is_a_strict_no_op() -> None:
    stream = io.StringIO()
    pane = FramePane(stream)
    pane.draw(_LINES)
    pane.restore()
    assert stream.getvalue() == ""


def test_frame_pane_broken_isatty_fails_closed_to_no_op() -> None:
    stream = _BrokenIsattyStream()
    pane = FramePane(stream)
    pane.draw(_LINES)
    pane.restore()
    assert stream.getvalue() == ""


def test_frame_pane_first_draw_clears_screen_and_hides_cursor() -> None:
    stream = _TtyStream()
    pane = FramePane(stream)
    pane.draw(_LINES)
    output = stream.getvalue()

    assert "\x1b[2J" in output
    assert "\x1b[?25l" in output
    for index, line in enumerate(_LINES):
        assert f"\x1b[{index + 1};1H{line}\x1b[K" in output
    # Anything below the frame is cleared, and nothing ever appends a
    # bare newline — every row is addressed by absolute position.
    assert "\x1b[J" in output
    assert "\n" not in output


def test_frame_pane_same_height_redraw_rewrites_only_changed_rows() -> None:
    stream = _TtyStream()
    pane = FramePane(stream)
    pane.draw(_LINES)
    stream.truncate(0)
    stream.seek(0)

    pane.draw(("alpha", "CHANGED", "gamma"))
    output = stream.getvalue()

    # No re-clear on a same-height draw.
    assert "\x1b[2J" not in output
    assert "\x1b[?25l" not in output
    # Only row 2 (the changed one) is rewritten.
    assert "\x1b[2;1HCHANGED\x1b[K" in output
    assert "\x1b[1;1H" not in output
    assert "\x1b[3;1H" not in output
    assert "\n" not in output


def test_frame_pane_identical_redraw_writes_only_a_cursor_park() -> None:
    stream = _TtyStream()
    pane = FramePane(stream)
    pane.draw(_LINES)
    stream.truncate(0)
    stream.seek(0)

    pane.draw(_LINES)
    output = stream.getvalue()

    # No row rewrite at all: just a park below the frame, no flicker,
    # and no growth in terminal output for a stream of identical ticks.
    assert output == f"\x1b[{len(_LINES) + 1};1H"


def test_frame_pane_repeated_identical_draws_add_zero_growth() -> None:
    stream = _TtyStream()
    pane = FramePane(stream)
    pane.draw(_LINES)
    stream.truncate(0)
    stream.seek(0)

    pane.draw(_LINES)
    pane.draw(_LINES)
    pane.draw(_LINES)
    output = stream.getvalue()
    park = f"\x1b[{len(_LINES) + 1};1H"
    assert output == park * 3


def test_frame_pane_changed_line_count_forces_a_full_reclear() -> None:
    stream = _TtyStream()
    pane = FramePane(stream)
    pane.draw(_LINES)
    stream.truncate(0)
    stream.seek(0)

    pane.draw((*_LINES, "delta"))
    output = stream.getvalue()
    assert "\x1b[2J" in output
    assert "\x1b[?25l" in output
    for index, line in enumerate((*_LINES, "delta")):
        assert f"\x1b[{index + 1};1H{line}\x1b[K" in output


def test_frame_pane_fresh_writer_reestablishes_idempotently() -> None:
    # Crash recovery: a fresh writer over the same stream re-emits the
    # deterministic full-clear establishment sequence, exactly like
    # DashboardPane's re-establishment story.
    stream = _TtyStream()
    FramePane(stream).draw(_LINES)
    FramePane(stream).draw(_LINES)
    assert stream.getvalue().count("\x1b[2J") == 2


def test_frame_pane_restore_shows_cursor_and_clears_screen() -> None:
    stream = _TtyStream()
    pane = FramePane(stream)
    pane.draw(_LINES)
    stream.truncate(0)
    stream.seek(0)

    pane.restore()
    output = stream.getvalue()
    assert "\x1b[?25h" in output
    assert "\x1b[0m" in output
    assert "\x1b[2J" in output


def test_frame_pane_next_draw_after_restore_reclears() -> None:
    stream = _TtyStream()
    pane = FramePane(stream)
    pane.draw(_LINES)
    pane.restore()
    stream.truncate(0)
    stream.seek(0)

    pane.draw(_LINES)
    output = stream.getvalue()
    assert "\x1b[2J" in output
    assert "\x1b[?25l" in output
