"""Exception hierarchy for plrtool.

``PlrtoolError`` is the base for all tool-specific failures (messages shown
as-is with a traceback, fail-fast).  ``ClusterError`` is raised when a
cluster/API problem should stop the whole run.
"""

from __future__ import annotations


class PlrtoolError(Exception):
    """Base error; messages are shown as-is (fail-fast, traceback shown)."""


class ClusterError(PlrtoolError):
    """Cluster/API access problem that should stop the whole run."""
