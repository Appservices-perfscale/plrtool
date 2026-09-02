"""Targets: selectable inputs, registry-based for future expansion.

Each selector owns the CLI flags it needs and the parsing of those flags.  To
add a new input source (e.g. --since-date) subclass ``TargetSelector`` and
register it in ``SELECTOR_CLASSES``.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

from .exceptions import PlrtoolError

__all__ = [
    "CsvSelector",
    "NamespacePlrSelector",
    "Target",
    "TargetSelector",
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

    def collect(self, args: argparse.Namespace) -> list[Target]:
        """Return targets from this selector given parsed CLI args."""
        raise NotImplementedError


class NamespacePlrSelector(TargetSelector):
    """--namespace NS --plr NAME: one specific PipelineRun."""

    def add_args(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--namespace", help="tenant namespace of the PipelineRun")
        parser.add_argument("--plr", help="PipelineRun name")

    def collect(self, args: argparse.Namespace) -> list[Target]:
        ns, name = getattr(args, "namespace", None), getattr(args, "plr", None)
        if not ns and not name:
            return []
        if not ns or not name:
            raise PlrtoolError("--namespace and --plr must be used together")
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

    def collect(self, args: argparse.Namespace) -> list[Target]:
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


SELECTOR_CLASSES: tuple = (NamespacePlrSelector, CsvSelector)


def get_selectors() -> list[TargetSelector]:
    """Instantiate all registered target selectors."""
    return [selector_class() for selector_class in SELECTOR_CLASSES]


def add_selector_args(parser: argparse.ArgumentParser) -> None:
    """Add CLI flags of all registered selectors to a subparser."""
    for selector in get_selectors():
        selector.add_args(parser)


def resolve_targets(args: argparse.Namespace) -> list[Target]:
    """Collect targets from all selectors, deduplicated, order preserved."""
    seen = set()
    targets: list[Target] = []
    for selector in get_selectors():
        for target in selector.collect(args):
            key = (target.namespace, target.plr)
            if key not in seen:
                seen.add(key)
                targets.append(target)
    if not targets:
        raise PlrtoolError("no targets given: use --namespace/--plr or a --csv file")
    return targets
