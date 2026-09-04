"""Shared constants: defaults, formats, ANSI colors.

Split out of the original single-file script section ``0. Constants`` so the
rest of the package can import them without pulling in logging or exceptions.
"""

from __future__ import annotations

import os
from pathlib import Path

LOG_FILENAME = "plrtool.log"
DEFAULT_LOG_DIR_ENV = "PLR_LOG_DIR"
# XDG data dir (~/.local/share) by default, like other user data locations.
DEFAULT_LOG_DIR = os.environ.get(DEFAULT_LOG_DIR_ENV) or str(
    Path.home() / ".local" / "share" / "plrtool"
)
DEFAULT_LOG_FILE = str(Path(DEFAULT_LOG_DIR) / LOG_FILENAME)
DEFAULT_CACHE_ENV = "PLR_CACHE_DIR"
DEFAULT_CACHE = "collected-data"
DEFAULT_CONCURRENCY = 8
DEFAULT_TIMEOUT = "100m"
DEFAULT_DELETE_TIMEOUT = "1m"
RETRIES = 3
RETRY_SLEEP = 1.0
POLL_INTERVAL = 5.0
MISSING = "missing"

TD_FMT = "%Y-%m-%dT%H:%M:%SZ"

YELLOW = "\x1b[33m"
BLUE = "\x1b[34m"
RED = "\x1b[31m"
RESET = "\x1b(B\x1b[m"
