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
