"""Normalize a PipelineRun manifest so two runs can be diffed cleanly.

Run-specific data (``status``, object UIDs, resource versions, creation
timestamps, managed fields, owner references) is dropped, map keys are sorted
and lists of named objects are sorted by name.  ``--params-only`` prints just
``spec.params`` for a quick parameter diff.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

from .cache import strip_managed_fields
from .exceptions import PlrtoolError

__all__ = ["cmd_normalize", "load_manifest", "normalize_plr"]
# ---------------------------------------------------------------------------
# 10. Normalize manifests for diffing (CLI section follows in cli.py)
# ---------------------------------------------------------------------------

VOLATILE_METADATA_KEYS = frozenset(
    {
        "creationTimestamp",
        "deletionGracePeriodSeconds",
        "deletionTimestamp",
        "generation",
        "managedFields",
        "ownerReferences",
        "resourceVersion",
        "selfLink",
        "uid",
    }
)

VOLATILE_ANNOTATIONS = frozenset({"kubectl.kubernetes.io/last-applied-configuration"})


def load_manifest(path: Path) -> dict:
    """Read a YAML/JSON manifest, unwrapping the legacy ``{items:[...]}`` shape."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise PlrtoolError(f"cannot read {path}: {exc}") from exc
    try:
        document = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise PlrtoolError(f"{path}: invalid YAML: {exc}") from exc
    if not isinstance(document, dict):
        raise PlrtoolError(f"{path}: expected a mapping, got {type(document).__name__}")
    items = document.get("items")
    if isinstance(items, list) and items:
        document = items[0]
    if not isinstance(document, dict):
        raise PlrtoolError(f"{path}: expected a mapping inside 'items'")
    return document


def _clean_metadata(metadata: dict) -> dict:
    """Drop volatile metadata fields and the kubectl last-applied annotation."""
    cleaned = {key: value for key, value in metadata.items() if key not in VOLATILE_METADATA_KEYS}
    annotations = cleaned.get("annotations")
    if isinstance(annotations, dict):
        kept = {key: value for key, value in annotations.items() if key not in VOLATILE_ANNOTATIONS}
        if kept:
            cleaned["annotations"] = kept
        else:
            cleaned.pop("annotations")
    return cleaned


def _sort_named(items: list) -> list:
    """Sort a list of mappings by ``name`` when every item has one."""
    if items and all(
        isinstance(item, dict) and isinstance(item.get("name"), str) for item in items
    ):
        return sorted(items, key=lambda item: item["name"])
    return items


def _normalize_value(value: object) -> object:
    """Recursively sort maps and lists of named objects; drop nothing else."""
    if isinstance(value, dict):
        return {
            key: _normalize_value(item) for key, item in sorted(value.items(), key=lambda kv: kv[0])
        }
    if isinstance(value, list):
        return _sort_named([_normalize_value(item) for item in value])
    return value


def normalize_plr(raw: dict, params_only: bool = False) -> object:
    """Return a deterministic, run-independent view of a PipelineRun manifest."""
    cleaned = strip_managed_fields(raw)
    assert isinstance(cleaned, dict)
    if params_only:
        return _normalize_value((cleaned.get("spec") or {}).get("params") or [])
    normalized = {key: value for key, value in cleaned.items() if key != "status"}
    metadata = normalized.get("metadata")
    if isinstance(metadata, dict):
        normalized["metadata"] = _clean_metadata(metadata)
    return _normalize_value(normalized)


def cmd_normalize(args: argparse.Namespace) -> int:
    """CLI entry for 'normalize': print the normalized manifest to stdout."""
    raw = load_manifest(Path(args.file))
    normalized = normalize_plr(raw, params_only=args.params_only)
    sys.stdout.write(
        yaml.safe_dump(normalized, sort_keys=False, default_flow_style=False, allow_unicode=True)
    )
    return 0
