"""Options dataclasses: thinnest possible arg plumbing (CLI -> logic)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .constants import DEFAULT_CACHE, DEFAULT_CONCURRENCY, RETRIES, RETRY_SLEEP

__all__ = ["DownloadOptions", "TimingOptions", "WaitOptions"]
# ---------------------------------------------------------------------------
# 2. Options dataclasses (thinnest possible arg plumbing: CLI -> logic)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DownloadOptions:
    """Options for the download subcommand / shared collect helpers."""

    cache_dir: Path = Path(DEFAULT_CACHE)
    concurrency: int = DEFAULT_CONCURRENCY
    also_incomplete: bool = False
    details_if_failed: bool = False
    details_included: bool = False
    retries: int = RETRIES
    retry_sleep: float = RETRY_SLEEP
    with_timing: TimingOptions | None = None
    with_errors: bool = False
    ka_context: str | None = None
    ka_conf: str | None = None


@dataclass(frozen=True)
class WaitOptions:
    """Options for the wait subcommand."""

    cache_dir: Path = Path(DEFAULT_CACHE)
    concurrency: int = DEFAULT_CONCURRENCY
    timeout: float = 0.0
    dump_completed: bool = False


@dataclass(frozen=True)
class TimingOptions:
    """Options for timing analysis (standalone or --with-timing passthrough)."""

    gantt_chart: str | None = None
    summary: str | None = None
    details: bool = False
