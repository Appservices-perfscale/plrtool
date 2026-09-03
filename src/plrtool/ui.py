"""Shared console output helpers (unified section-header style for all subcommands)."""

from __future__ import annotations

from .constants import RESET, YELLOW

__all__ = ["section"]


def section(title: str, count: int | str | None = None) -> None:
    """Print a yellow '=== Title (count) ===' section header (shared style)."""
    suffix = f" ({count})" if count is not None else ""
    print(f"{YELLOW}=== {title}{suffix} ==={RESET}")
