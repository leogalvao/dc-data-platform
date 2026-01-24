"""
Console output utilities with optional rich support.

Provides graceful fallback when rich is not installed.
"""

from __future__ import annotations

import sys
from typing import Any

# Try to import rich
try:
    from rich.console import Console as RichConsole
    from rich.panel import Panel
    from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn
    from rich.table import Table
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


class Console:
    """
    Console output wrapper with rich fallback.

    Uses rich for pretty output if available,
    falls back to plain print otherwise.
    """

    def __init__(self, force_plain: bool = False):
        """
        Initialize console.

        Args:
            force_plain: Use plain output even if rich is available
        """
        self._use_rich = RICH_AVAILABLE and not force_plain
        if self._use_rich:
            self._console = RichConsole()
        else:
            self._console = None

    def print(self, *args: Any, **kwargs: Any) -> None:
        """Print to console."""
        if self._use_rich:
            self._console.print(*args, **kwargs)
        else:
            print(*args)

    def print_table(
        self,
        headers: list[str],
        rows: list[list[Any]],
        title: str | None = None,
    ) -> None:
        """Print a table."""
        if self._use_rich:
            table = Table(title=title)
            for header in headers:
                table.add_column(header)
            for row in rows:
                table.add_row(*[str(cell) for cell in row])
            self._console.print(table)
        else:
            if title:
                print(f"\n{title}")
                print("=" * len(title))
            # Simple table formatting
            col_widths = [len(h) for h in headers]
            for row in rows:
                for i, cell in enumerate(row):
                    col_widths[i] = max(col_widths[i], len(str(cell)))

            # Print header
            header_line = " | ".join(
                h.ljust(col_widths[i]) for i, h in enumerate(headers)
            )
            print(header_line)
            print("-" * len(header_line))

            # Print rows
            for row in rows:
                row_line = " | ".join(
                    str(cell).ljust(col_widths[i]) for i, cell in enumerate(row)
                )
                print(row_line)

    def print_panel(self, content: str, title: str | None = None) -> None:
        """Print content in a panel/box."""
        if self._use_rich:
            self._console.print(Panel(content, title=title))
        else:
            width = max(len(line) for line in content.split("\n")) + 4
            if title:
                print(f"╭─ {title} " + "─" * (width - len(title) - 4) + "╮")
            else:
                print("╭" + "─" * width + "╮")
            for line in content.split("\n"):
                print(f"│ {line.ljust(width - 2)} │")
            print("╰" + "─" * width + "╯")

    def status(self, message: str) -> Any:
        """Create a status context manager."""
        if self._use_rich:
            return self._console.status(message)
        else:
            return PlainStatus(message)


class PlainStatus:
    """Plain status context manager (no-op)."""

    def __init__(self, message: str):
        self.message = message

    def __enter__(self):
        print(f"[...] {self.message}")
        return self

    def __exit__(self, *args):
        print(f"[OK] {self.message}")


def create_progress_bar(
    total: int,
    description: str = "Processing",
) -> Any:
    """
    Create a progress bar.

    Args:
        total: Total items
        description: Progress description

    Returns:
        Progress bar context manager
    """
    if RICH_AVAILABLE:
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
        )
        return RichProgressWrapper(progress, total, description)
    else:
        return PlainProgressBar(total, description)


class RichProgressWrapper:
    """Wrapper for rich progress bar."""

    def __init__(self, progress: Any, total: int, description: str):
        self._progress = progress
        self._total = total
        self._description = description
        self._task_id = None

    def __enter__(self):
        self._progress.start()
        self._task_id = self._progress.add_task(self._description, total=self._total)
        return self

    def __exit__(self, *args):
        self._progress.stop()

    def update(self, advance: int = 1) -> None:
        """Update progress."""
        self._progress.update(self._task_id, advance=advance)


class PlainProgressBar:
    """Simple text-based progress bar."""

    def __init__(self, total: int, description: str):
        self._total = total
        self._description = description
        self._current = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        print()  # Newline after progress

    def update(self, advance: int = 1) -> None:
        """Update progress."""
        self._current += advance
        percent = (self._current / self._total) * 100 if self._total > 0 else 0
        bar_width = 30
        filled = int(bar_width * self._current / self._total) if self._total > 0 else 0
        bar = "█" * filled + "░" * (bar_width - filled)
        sys.stdout.write(f"\r{self._description}: [{bar}] {percent:.1f}% ({self._current}/{self._total})")
        sys.stdout.flush()
