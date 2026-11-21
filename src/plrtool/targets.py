"""Targets: selectable inputs, registry-based for future expansion.

Each selector owns the CLI flags it needs and the parsing of those flags.  To
add a new input source (e.g. --since-date) subclass ``TargetSelector`` and
register it in ``SELECTOR_CLASSES``.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from .exceptions import PlrtoolError
from .utils import parse_iso_datetime

if TYPE_CHECKING:
    from .cluster import Cluster

__all__ = [
    "CsvSelector",
    "NamespacePlrSelector",
    "Target",
    "TargetSelector",
    "TimeRangeSelector",
    "add_selector_args",
    "get_selectors",
    "resolve_targets",
]
# ---------------------------------------------------------------------------
# 3. Targets: selectable inputs, registry-based for future expansion
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Target:
    """A single PipelineRun the tool should operate on."""

    namespace: str
    plr: str


class TargetSelector:
    """Base class: a source of PLR targets.

    Each selector owns the CLI flags it needs (added via add_args) and the
    parsing of those flags (collect).  To add a new input source (e.g.
    --since-date) subclass TargetSelector, register it in SELECTOR_CLASSES and
    collect_targets() picks it up automatically.
    """

    def add_args(self, parser: argparse.ArgumentParser) -> None:
        """Declare the selector's own CLI flags on the given subparser."""
        raise NotImplementedError

    def collect(self, args: argparse.Namespace, cluster: Cluster | None = None) -> list[Target]:
        """Return targets from this selector given parsed CLI args.

        ``cluster`` gives selectors that need cluster access (listing by time
        range) a client; selectors that only parse args ignore it.
        """
        raise NotImplementedError


class NamespacePlrSelector(TargetSelector):
    """--namespace NS --plr NAME: one specific PipelineRun."""

    def add_args(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--namespace", help="tenant namespace of the PipelineRun")
        parser.add_argument("--plr", help="PipelineRun name")

    def collect(self, args: argparse.Namespace, cluster: Cluster | None = None) -> list[Target]:
        ns, name = getattr(args, "namespace", None), getattr(args, "plr", None)
        if not ns and not name:
            return []
        if not ns:
            raise PlrtoolError("--plr requires --namespace")
        if not name:
            # --namespace without --plr is owned by the time-range selector.
            return []
        return [Target(ns, name)]


class CsvSelector(TargetSelector):
    """--csv FILE: rows '<namespace>,<plr_name>'.

    Also tolerates the legacy loadtest format used by plrs.list:
    '<namespace> pipelinerun.tekton.dev/<name> created'.
    """

    def add_args(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--csv",
            metavar="FILE",
            help="CSV file with rows '<namespace>,<plr_name>' (also accepts "
            "legacy '<ns> pipelinerun.tekton.dev/<name> created' lines)",
        )

    def collect(self, args: argparse.Namespace, cluster: Cluster | None = None) -> list[Target]:
        csv_path = getattr(args, "csv", None)
        if not csv_path:
            return []
        path = Path(csv_path)
        if not path.is_file():
            raise PlrtoolError(f"--csv file not found: {path}")
        targets: list[Target] = []
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "," in line:
                parts = [part.strip() for part in line.split(",")]
                if len(parts) != 2 or not parts[0] or not parts[1]:
                    raise PlrtoolError(f"{path}:{lineno}: malformed CSV row: {line!r}")
                namespace, name = parts
            else:
                parts = line.split()
                if len(parts) < 2:
                    raise PlrtoolError(f"{path}:{lineno}: malformed row: {line!r}")
                namespace, name = parts[0], parts[1]
            name = name.split("/")[-1]  # tolerate 'pipelinerun.tekton.dev/NAME'
            targets.append(Target(namespace, name))
        return targets


class TimeRangeSelector(TargetSelector):
    """--namespace NS --plr-since/--plr-until: every PLR created in a time range.

    Both bounds are optional (open-ended); each is an ISO datetime (date-only
    allowed, then midnight UTC).  A PLR matches when it was created strictly
    after --plr-since and strictly before --plr-until.  Targets are discovered
    by listing PipelineRuns from the live cluster and KubeArchive, so a
    Cluster must be passed to resolve_targets().  Combinable with the other
    selectors: results are unioned and deduplicated.
    """

    def add_args(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--plr-since",
            metavar="ISODATE",
            help="select PLRs created after this ISO datetime (requires --namespace)",
        )
        parser.add_argument(
            "--plr-until",
            metavar="ISODATE",
            help="select PLRs created before this ISO datetime (requires --namespace)",
        )

    def collect(self, args: argparse.Namespace, cluster: Cluster | None = None) -> list[Target]:
        since_raw = getattr(args, "plr_since", None)
        until_raw = getattr(args, "plr_until", None)
        if not since_raw and not until_raw:
            return []
        namespace = getattr(args, "namespace", None)
        if not namespace:
            raise PlrtoolError("--plr-since/--plr-until require --namespace")
        since = parse_iso_datetime(since_raw) if since_raw else None
        until = parse_iso_datetime(until_raw) if until_raw else None
        if since is not None and until is not None and since >= until:
            raise PlrtoolError(
                f"--plr-since must be before --plr-until ({since_raw} >= {until_raw})"
            )
        if cluster is None:
            raise PlrtoolError("--plr-since/--plr-until need cluster access")
        names = cluster.list_pipelinerun_names(namespace, since, until)
        if not names:
            raise PlrtoolError(
                f"no PipelineRuns found in namespace {namespace!r} "
                f"created after {since_raw or 'the beginning'} and before {until_raw or 'now'}"
            )
        return [Target(namespace, name) for name in names]


SELECTOR_CLASSES: tuple = (NamespacePlrSelector, CsvSelector, TimeRangeSelector)


def get_selectors() -> list[TargetSelector]:
    """Instantiate all registered target selectors."""
    return [selector_class() for selector_class in SELECTOR_CLASSES]


def add_selector_args(parser: argparse.ArgumentParser) -> None:
    """Add CLI flags of all registered selectors to a subparser."""
    for selector in get_selectors():
        selector.add_args(parser)


def resolve_targets(args: argparse.Namespace, cluster: Cluster | None = None) -> list[Target]:
    """Collect targets from all selectors, deduplicated, order preserved.

    ``cluster`` is forwarded to the selectors (needed by the ones that list
    from the cluster, e.g. the time-range selector).
    """
    seen = set()
    targets: list[Target] = []
    for selector in get_selectors():
        for target in selector.collect(args, cluster):
            key = (target.namespace, target.plr)
            if key not in seen:
                seen.add(key)
                targets.append(target)
    if not targets:
        raise PlrtoolError(
            "no targets given: use --namespace/--plr, a --csv file, "
            "or --namespace with --plr-since/--plr-until"
        )
    return targets
