"""Pure utilities: time parsing, percentiles, durations, message normalization.

No kubernetes/matplotlib deps; the analysis modules build on these.
"""

from __future__ import annotations

import datetime as dt
import re

from .constants import MISSING, TD_FMT
from .exceptions import PlrtoolError

__all__ = [
    "duration_seconds",
    "epoch_of",
    "fmt_ts",
    "normalize_message",
    "parse_duration",
    "parse_ts_dt",
    "percentile",
]
# ---------------------------------------------------------------------------
# 1. Pure utilities: time, percentiles, messages
# ---------------------------------------------------------------------------


def parse_ts_dt(value: object) -> dt.datetime | None:
    """Parse an RFC3339-ish timestamp into an aware UTC datetime, or None."""
    if value is None:
        return None
    text = str(value).strip()
    if not text or text in ("n/a", "null"):
        return None
    try:
        parsed = dt.datetime.fromisoformat(text)  # 3.11+ accepts 'Z'
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC)


def fmt_ts(value: dt.datetime | None) -> str:
    """Format a datetime as 'YYYY-MM-DDTHH:MM:SSZ' (UTC) or 'n/a'."""
    if value is None:
        return "n/a"
    return value.astimezone(dt.UTC).strftime(TD_FMT)


def epoch_of(value: dt.datetime | None) -> int | None:
    """Seconds since epoch (UTC), or None."""
    if value is None:
        return None
    return int(value.timestamp())


def duration_seconds(start: dt.datetime | None, end: dt.datetime | None) -> int | None:
    """Whole seconds between two datetimes, or None when either is missing."""
    if start is None or end is None:
        return None
    return round((end - start).total_seconds())


def percentile(values: list, p: int = 99) -> int | None:
    """Nearest-rank percentile, matching the original bash computation.

    rank = ceil(p/100 * n); uses the same integer arithmetic the original
    check-timings.sh used (rank=(99*n+99)//100 for p=99).
    """
    ordered = sorted(values)
    if not ordered:
        return None
    rank = (p * len(ordered) + 99) // 100
    return ordered[rank - 1]


def parse_duration(text: str) -> float:
    """Parse a duration like '30s', '100m', '2h', '1h30m' into seconds."""
    total = 0.0
    for match in re.finditer(r"(\d+(?:\.\d+)?)\s*([hms])", str(text)):
        value = float(match.group(1))
        total += value * {"h": 3600.0, "m": 60.0, "s": 1.0}[match.group(2)]
    if total <= 0:
        raise PlrtoolError(f"invalid duration: {text!r}")
    return total


NORMALIZERS = (
    (re.compile(r"test-rhtap-[0-9]+-tenant"), "test-rhtap-...-tenant"),
    (re.compile(r"load-test-[0-9]+-[a-z0-9]+"), "load-test-..."),
    (re.compile(r"\b[0-9a-f]{10,}\b"), "..."),
)


def normalize_message(message: object) -> str:
    """Strip run-specific tokens from a message so equal failures bucket together."""
    if message is None:
        return MISSING
    normalized = str(message)
    for pattern, replacement in NORMALIZERS:
        normalized = pattern.sub(replacement, normalized)
    return normalized.strip()
