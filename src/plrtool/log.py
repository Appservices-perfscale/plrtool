"""Logging setup (root handler wiring).

The rest of the package obtains its logger via ``logging.getLogger("plrtool")``,
so a single ``setup_logging`` call configures every module's logger.
"""

from __future__ import annotations

import logging
import logging.handlers
import sys

logger = logging.getLogger("plrtool")


def setup_logging(log_file: str, verbose: bool = False, debug: bool = False) -> logging.Logger:
    """Configure root logging: DEBUG to file, WARNING (or INFO/DEBUG) to stderr.

    stderr level: WARNING by default, INFO with --verbose, DEBUG with --debug.
    The log file always receives DEBUG so internals are recoverable.
    """
    stderr_level = logging.DEBUG if debug else (logging.INFO if verbose else logging.WARNING)
    root = logging.getLogger()
    if root.handlers:
        # Already configured (e.g. tests call main twice); keep handlers.
        return logger
    root.setLevel(logging.DEBUG)
    stderr = logging.StreamHandler(sys.stderr)
    stderr.setLevel(stderr_level)
    stderr.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    root.addHandler(stderr)
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=5_000_000, backupCount=3, encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root.addHandler(file_handler)
    return logger
