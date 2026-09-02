"""CacheStore: on-disk JSON cache + in-memory records.

On disk: full JSON of each fetched object (managedFields stripped) so the
cache stays a complete archive.  In memory: only the extracted record subset
(PLRRecord/TaskRunRecord/PodRecord) so analysis is fast and light.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterator
from pathlib import Path

import yaml

from .records import PLRRecord, PodRecord, TaskRunRecord, parse_plr, parse_pod, parse_taskrun

logger = logging.getLogger("plrtool")

__all__ = ["CacheStore", "strip_managed_fields"]
# ---------------------------------------------------------------------------
# 6. CacheStore: on-disk JSON cache + in-memory records
# ---------------------------------------------------------------------------


def strip_managed_fields(obj) -> object:
    """Recursively remove every 'managedFields' key (metadata bloat)."""
    if isinstance(obj, dict):
        return {
            key: strip_managed_fields(value) for key, value in obj.items() if key != "managedFields"
        }
    if isinstance(obj, list):
        return [strip_managed_fields(item) for item in obj]
    return obj


def _load_doc(path: Path) -> dict | None:
    """Load a cached JSON/YAML file into a single object dict.

    Newer dumps are plain objects; legacy cache files wrap the object in an
    {"items": [...]} list (the oc/kubectl get -o yaml shape).  The wrapper is
    unwrapped to items[0] as a fallback.  An empty items list yields None and
    more than one item emits a warning (only the first is used).
    """
    try:
        if path.suffix == ".json":
            document = json.loads(path.read_text(encoding="utf-8"))
        else:
            document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        logger.warning("failed to read %s: %s", path, exc)
        return None
    if not isinstance(document, dict):
        logger.warning("%s: expected a mapping, got %s", path, type(document).__name__)
        return None
    items = document.get("items")
    if items is None:
        return document
    if isinstance(items, list):
        if not items:
            logger.warning("%s: empty 'items' list; treating as missing", path)
            return None
        if len(items) > 1:
            logger.warning("%s: %d items in 'items' list; using items[0] only", path, len(items))
        return items[0]
    logger.warning(
        "%s: 'items' is %s, not a list; treating whole document as the object",
        path,
        type(items).__name__,
    )
    return document


class CacheStore:
    """Cache directory + in-memory record maps.

    On disk: full JSON of each fetched object (managedFields stripped) so the
    cache stays a complete archive.  In memory: only the extracted record
    subset (PLRRecord/TaskRunRecord/PodRecord) so analysis is fast and light.
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self.plrs: dict[str, PLRRecord] = {}
        self.taskruns: dict[str, TaskRunRecord] = {}
        self.pods: dict[str, PodRecord] = {}

    def ensure_dir(self) -> None:
        """Create the cache directory if needed."""
        self.path.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _atomic_write(path: Path, content: str) -> None:
        """Write via a temp file + rename so concurrent readers never see partial."""
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(content, encoding="utf-8")
        os.replace(tmp, path)

    def _dump_json(self, filename: str, raw: dict) -> None:
        self.ensure_dir()
        cleaned = strip_managed_fields(raw)
        self._atomic_write(self.path / filename, json.dumps(cleaned, indent=1) + "\n")

    def add_plr(self, raw: dict, namespace: str | None = None) -> PLRRecord:
        """Dump a PipelineRun to JSON and register its record in memory."""
        record = parse_plr(raw, namespace)
        self.plrs[record.name] = record
        self._dump_json(f"collected-pipelinerun-{record.name}.json", raw)
        return record

    def add_taskrun(self, raw: dict, namespace: str | None = None) -> TaskRunRecord:
        """Dump a TaskRun to JSON and register its record in memory."""
        record = parse_taskrun(raw, namespace)
        self.taskruns[record.name] = record
        self._dump_json(f"collected-taskrun-{record.name}.json", raw)
        return record

    def add_pod(self, raw: dict, namespace: str | None = None) -> PodRecord:
        """Dump a Pod to JSON and register its record in memory."""
        record = parse_pod(raw, namespace)
        self.pods[record.name] = record
        self._dump_json(f"collected-pod-{record.name}.json", raw)
        return record

    def add_log(self, pod: str, container: str, text: str) -> None:
        """Dump a container log file (logs are never kept in memory)."""
        self.ensure_dir()
        self._atomic_write(self.path / f"pod-{pod}-{container}.log", text)

    def read_cached_object(self, prefix: str, name: str) -> dict | None:
        """Return an already-cached object (JSON preferred, legacy YAML), else None.

        Matches fetch-or-cached.sh's "cached files win" semantics: when a file
        for this object already exists, do not touch the cluster.
        """
        for suffix in (".json", ".yaml"):
            path = self.path / f"collected-{prefix}-{name}{suffix}"
            if path.is_file():
                doc = _load_doc(path)
                if doc is not None:
                    return doc
                logger.warning("cached file unreadable, will try cluster: %s", path)
                return None
        return None

    def read_cached_log(self, pod: str, container: str) -> str | None:
        """Return an already-cached container log, else None."""
        path = self.path / f"pod-{pod}-{container}.log"
        if not path.is_file():
            return None
        try:
            return path.read_text(encoding="utf-8")
        except OSError as exc:
            logger.warning("cached log unreadable: %s: %s", path, exc)
            return None

    def _objects(self, prefix: str) -> Iterator[tuple[Path, dict]]:
        """Yield (path, unwrapped-doc) for cached objects of one kind.

        JSON files are preferred; legacy .yaml files are only read when no
        JSON twin exists so old run directories keep working.
        """
        json_files = sorted(self.path.glob(f"collected-{prefix}-*.json"))
        yaml_files = sorted(self.path.glob(f"collected-{prefix}-*.yaml"))
        json_stems = {f.stem for f in json_files}
        for path in json_files:
            doc = _load_doc(path)
            if doc is not None:
                yield path, doc
        for path in yaml_files:
            if path.stem in json_stems:
                continue
            doc = _load_doc(path)
            if doc is not None:
                yield path, doc

    def load(self) -> CacheStore:
        """Populate in-memory records from the cache directory. Returns self."""
        self.plrs.clear()
        self.taskruns.clear()
        self.pods.clear()
        for _path, doc in self._objects("pipelinerun"):
            self.plrs[doc.get("metadata", {}).get("name") or _path.stem] = parse_plr(doc)
        for _path, doc in self._objects("taskrun"):
            self.taskruns[doc.get("metadata", {}).get("name") or _path.stem] = parse_taskrun(doc)
        for _path, doc in self._objects("pod"):
            self.pods[doc.get("metadata", {}).get("name") or _path.stem] = parse_pod(doc)
        # Link TaskRuns and Pods onto PLRs for per-run analysis.
        for plr in self.plrs.values():
            for tr_name in plr.tr_refs:
                tr = self.taskruns.get(tr_name)
                if tr is not None:
                    plr.taskruns.append(tr)
                    if tr.pod_name:
                        tr.pod = self.pods.get(tr.pod_name)
        return self
