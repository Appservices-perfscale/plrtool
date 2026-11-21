"""Logging setup (root handler wiring).

The rest of the package obtains its logger via ``logging.getLogger("plrtool")``,
so a single ``setup_logging`` call configures every module's logger.

Logging convention (keep this in sync when adding log calls)
------------------------------------------------------------

- ``ERROR``   - a target failed and counts against the exit code
               (collection failed, delete failed).
- ``WARNING`` - data gaps the user must know about even without flags
               (missing TaskRun/Pod/logs, unreadable cache file, partial
               backend results).
- ``INFO``    - progress milestones, visible with ``--verbose``: phase
               starts/ends, one line per completed PLR with a running
               ``[done/total]`` count, final summaries.
- ``DEBUG``   - per-API-call chatter, visible with ``--debug`` and always
               written to the log file: every object fetched or taken from
               the cache, every list page, 404 fallbacks, retries.

Rule of thumb: every network operation logs exactly once - success at
DEBUG (milestones at INFO), failure at WARNING or ERROR.  A silent run
must mean nothing happened; default (no flags) stderr shows WARNING+.
"""

from __future__ import annotations

import logging
import logging.handlers
import sys
from pathlib import Path

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
    log_path = Path(log_file).expanduser()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.handlers.RotatingFileHandler(
        log_path, maxBytes=5_000_000, backupCount=3, encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root.addHandler(file_handler)
    return logger
