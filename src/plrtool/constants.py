"""Shared constants: defaults, formats, ANSI colors.

Split out of the original single-file script section ``0. Constants`` so the
rest of the package can import them without pulling in logging or exceptions.
"""

from __future__ import annotations

LOG_FILENAME = "plrtool.log"
DEFAULT_CACHE_ENV = "PLR_CACHE_DIR"
DEFAULT_CACHE = "collected-data"
DEFAULT_CONCURRENCY = 8
DEFAULT_TIMEOUT = "100m"
RETRIES = 3
RETRY_SLEEP = 1.0
POLL_INTERVAL = 5.0
MISSING = "missing"

TD_FMT = "%Y-%m-%dT%H:%M:%SZ"

YELLOW = "\x1b[33m"
BLUE = "\x1b[34m"
RED = "\x1b[31m"
RESET = "\x1b(B\x1b[m"
