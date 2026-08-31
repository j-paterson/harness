"""ANSI scroll-region writer for the single Orchestrator surface.

cmux's closed CLI vocabulary has no pane-split verb (cmux.py) and is
deliberately not extended: both dashboard regions live inside the one
Orchestrator surface. A DECSTBM scroll region confines normal stdout
scrolling to the lower (classic Hermes) region while the upper
dashboard block is redrawn in place under cursor save/restore. When
stdout is not a TTY every operation is a strict no-op, and a fresh
writer re-establishes the region idempotently, which is the entire
crash-recovery story.
"""

from __future__ import annotations

import shutil
import sys
from collections.abc import Callable, Sequence
from typing import TextIO

_SAVE_CURSOR = "\x1b7"
_RESTORE_CURSOR = "\x1b8"
_CLEAR_TO_EOL = "\x1b[K"
_RESET_SCROLL_REGION = "\x1b[r"


def _terminal_rows() -> int:
    return shutil.get_terminal_size().lines


class DashboardPane:
    """Redraw an upper dashboard block without disturbing lower scroll."""

    def __init__(
        self,
        stream: TextIO | None = None,
        *,
        rows: Callable[[], int] | None = None,
    ) -> None:
        self._stream = stream if stream is not None else sys.stdout
        self._rows = rows or _terminal_rows
        self._established_height: int | None = None

    def draw(self, lines: Sequence[str]) -> None:
        """Establish the region as needed and redraw the block in place."""

        if not self._is_tty():
            return
        height = len(lines)
        if self._established_height != height:
            self._establish(height)
        frame = [_SAVE_CURSOR]
        for index, line in enumerate(lines):
            frame.append(f"\x1b[{index + 1};1H{line}{_CLEAR_TO_EOL}")
        frame.append(_RESTORE_CURSOR)
        self._write("".join(frame))

    def restore(self) -> None:
        """Reset the scroll region so the whole surface scrolls again."""

        if not self._is_tty():
            return
        self._established_height = None
        self._write(_RESET_SCROLL_REGION)

    def _establish(self, height: int) -> None:
        # DECSTBM confines normal scrolling to the lower region; the
        # cursor is then parked on the last row so daemon stdout keeps
        # scrolling below the dashboard block.
        total_rows = max(int(self._rows()), height + 1)
        self._established_height = height
        self._write(f"\x1b[{height + 1};{total_rows}r\x1b[{total_rows};1H")

    def _is_tty(self) -> bool:
        try:
            return bool(self._stream.isatty())
        except Exception:
            return False

    def _write(self, text: str) -> None:
        self._stream.write(text)
        self._stream.flush()


_HIDE_CURSOR = "\x1b[?25l"
_SHOW_CURSOR = "\x1b[?25h"
_CLEAR_SCREEN = "\x1b[2J"
_CLEAR_BELOW = "\x1b[J"
_RESET_ATTRIBUTES = "\x1b[0m"


class FramePane:
    """Redraw ONE full frame in place; nothing ever scrolls or appends.

    INFRA-209: unlike `DashboardPane`'s scroll region (which shares the
    surface with a second, independently scrolling region), this pane
    owns the whole stream. The first draw, and any draw whose line
    count changed, clears the screen and repaints every row. A same-
    height draw instead diffs against the last drawn lines and
    rewrites only the rows whose text actually changed — a run of
    identical frames (the common case between events) writes nothing
    but a cursor-park, so nothing ever flickers or grows terminal
    output. When stdout is not a TTY every operation is a strict
    no-op, and a fresh writer over the same stream re-establishes
    idempotently — the crash-recovery story is identical to
    `DashboardPane`.
    """

    def __init__(self, stream: TextIO | None = None) -> None:
        self._stream = stream if stream is not None else sys.stdout
        self._established_height: int | None = None
        self._last_lines: tuple[str, ...] | None = None

    def draw(self, lines: Sequence[str]) -> None:
        """Redraw only the rows of the frame that changed."""

        if not self._is_tty():
            return
        lines = tuple(lines)
        height = len(lines)
        if self._established_height != height:
            parts = [f"{_CLEAR_SCREEN}{_HIDE_CURSOR}"]
            for index, line in enumerate(lines):
                parts.append(f"\x1b[{index + 1};1H{line}{_CLEAR_TO_EOL}")
            parts.append(_CLEAR_BELOW)
            self._established_height = height
            self._last_lines = lines
            self._write("".join(parts))
            return

        assert self._last_lines is not None  # height unchanged => was set
        parts = [
            f"\x1b[{index + 1};1H{line}{_CLEAR_TO_EOL}"
            for index, (previous, line) in enumerate(
                zip(self._last_lines, lines, strict=True)
            )
            if previous != line
        ]
        self._last_lines = lines
        if parts:
            self._write("".join(parts))
        else:
            # No row changed: park the cursor below the frame so it
            # never blinks inside it, without rewriting anything.
            self._write(f"\x1b[{height + 1};1H")

    def restore(self) -> None:
        """Show the cursor and clear the screen; forget drawn state."""

        if not self._is_tty():
            return
        self._established_height = None
        self._last_lines = None
        self._write(f"{_SHOW_CURSOR}{_RESET_ATTRIBUTES}{_CLEAR_SCREEN}")

    def _is_tty(self) -> bool:
        try:
            return bool(self._stream.isatty())
        except Exception:
            return False

    def _write(self, text: str) -> None:
        self._stream.write(text)
        self._stream.flush()
