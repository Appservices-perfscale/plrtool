#!/usr/bin/env python3
"""plrtool - PipelineRun toolkit for kube-shard load tests.

Single-file CLI that replaces the shell/Python helpers in this directory
(collect-plrs.sh, collect-plr-artifacts.sh, fetch-or-cached.sh,
check-timings.py, check-plr-timings.sh, check-errors.py, check-plr-errors.sh,
wait-to-finish.sh).

Subcommands:

  download --cache DIR (--namespace NS --plr NAME | --csv FILE)
           [--concurrency N] [--also-incomplete]
           [--details-if-failed | --details-included]
           [--with-timing [--gantt-chart PATH] [--summary FILE]]
           [--with-errors] [--ka-context NAME] [--ka-conf FILE]
    Fetch PipelineRun manifests from the cluster API (live first, KubeArchive
    fallback), dump them as JSON into the cache (managedFields stripped),
    optionally fetch TaskRun/Pod manifests and container logs, and optionally
    run the timing/errors analysis on the in-memory data without re-reading.

  wait --cache DIR (--namespace NS --plr NAME | --csv FILE)
       [--concurrency N] [--timeout 100m] [--dump-completed]
    Wait for PLR(s) to complete (status.completionTime set).  With
    --dump-completed the completed PLR manifests are dumped via the shared
    download path.

  timing --cache DIR [--gantt-chart PATH] [--summary FILE]
    Analyze cached *succeeded* PipelineRuns; per-PLR breakdown plus aggregate
    stats.  Only files already in the cache are used (no cluster access).
    --gantt-chart renders a matplotlib Gantt chart; --summary writes the
    aggregate stats as JSON.  Both only when requested.

  errors --cache DIR
    Histogram condition/status/reason messages across the cache and classify
    the failure reason of each failed PipelineRun.

Concurrency, cache path and log verbosity all have sensible defaults.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import logging.handlers
import os
import re
import sys
import time
from collections import Counter, defaultdict
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

import yaml

# ---------------------------------------------------------------------------
# 0. Constants, exceptions, logging
# ---------------------------------------------------------------------------

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

logger = logging.getLogger("plrtool")


class PlrtoolError(Exception):
    """Base error; messages are shown as-is (fail-fast, traceback shown)."""


class ClusterError(PlrtoolError):
    """Cluster/API access problem that should stop the whole run."""


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


# ---------------------------------------------------------------------------
# 2. Options dataclasses (thinnest possible arg plumbing: CLI -> logic)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DownloadOptions:
    """Options for the download subcommand / shared collect helpers."""

    cache_dir: Path = Path(DEFAULT_CACHE)
    concurrency: int = DEFAULT_CONCURRENCY
    also_incomplete: bool = False
    details_if_failed: bool = False
    details_included: bool = False
    retries: int = RETRIES
    retry_sleep: float = RETRY_SLEEP
    with_timing: TimingOptions | None = None
    with_errors: bool = False
    ka_context: str | None = None
    ka_conf: str | None = None


@dataclass(frozen=True)
class WaitOptions:
    """Options for the wait subcommand."""

    cache_dir: Path = Path(DEFAULT_CACHE)
    concurrency: int = DEFAULT_CONCURRENCY
    timeout: float = 0.0
    dump_completed: bool = False


@dataclass(frozen=True)
class TimingOptions:
    """Options for timing analysis (standalone or --with-timing passthrough)."""

    gantt_chart: str | None = None
    summary: str | None = None


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


# ---------------------------------------------------------------------------
# 4. Records: subset of the cached data kept in memory
# ---------------------------------------------------------------------------


@dataclass
class StepRecord:
    """One step of a TaskRun (subset of status.steps[].terminated)."""

    name: str | None = None
    container: str | None = None
    started_at: dt.datetime | None = None
    finished_at: dt.datetime | None = None
    reason: str | None = None
    exit_code: int | None = None
    message: str | None = None


@dataclass
class ContainerStatusRecord:
    """One container status of a Pod (subset of containerStatuses[])."""

    name: str
    terminated_reason: str | None = None
    terminated_started: dt.datetime | None = None
    terminated_finished: dt.datetime | None = None
    terminated_exit_code: int | None = None
    waiting_reason: str | None = None


@dataclass
class PodRecord:
    """A Pod (subset of the Pod manifest)."""

    name: str
    namespace: str = ""
    node: str | None = None
    phase: str | None = None
    conditions: list = field(default_factory=list)  # (type, status, reason, time)
    containers: list[ContainerStatusRecord] = field(default_factory=list)


@dataclass
class TaskRunRecord:
    """A TaskRun (subset of the TaskRun manifest)."""

    name: str
    namespace: str = ""
    pipeline_task: str | None = None
    created: dt.datetime | None = None
    started: dt.datetime | None = None
    completed: dt.datetime | None = None
    succeeded_status: str | None = None
    succeeded_reason: str | None = None
    succeeded_message: str | None = None
    pod_name: str | None = None
    pod: PodRecord | None = None
    steps: list[StepRecord] = field(default_factory=list)
    sidecars: list = field(default_factory=list)  # container names


@dataclass
class PLRRecord:
    """A PipelineRun (subset of the PipelineRun manifest)."""

    name: str
    namespace: str = ""
    pipeline: str | None = None
    created: dt.datetime | None = None
    started: dt.datetime | None = None
    finally_start: dt.datetime | None = None
    completed: dt.datetime | None = None
    succeeded_status: str | None = None
    succeeded_reason: str | None = None
    succeeded_message: str | None = None
    tr_refs: list = field(default_factory=list)  # TaskRun names from childReferences
    taskruns: list[TaskRunRecord] = field(default_factory=list)
    skipped_stopping: int = 0  # task count skipped with "PipelineRun was stopping"


def succeeded_condition(item: dict) -> dict:
    """Return the status.conditions entry of type Succeeded (or {})."""
    for condition in (item.get("status") or {}).get("conditions") or []:
        if condition.get("type") == "Succeeded":
            return condition
    return {}


def parse_plr(raw: dict, namespace: str | None = None) -> PLRRecord:
    """Build a PLRRecord from a raw PipelineRun manifest."""
    meta = raw.get("metadata") or {}
    status = raw.get("status") or {}
    condition = succeeded_condition(raw)
    refs = [
        r.get("name")
        for r in status.get("childReferences") or []
        if r.get("kind") == "TaskRun" and r.get("name")
    ]
    skipped = sum(
        1
        for task in status.get("skippedTasks") or []
        if task.get("reason") == "PipelineRun was stopping"
    )
    return PLRRecord(
        name=meta.get("name") or "unknown",
        namespace=meta.get("namespace") or namespace or "",
        pipeline=(meta.get("labels") or {}).get("tekton.dev/pipeline"),
        created=parse_ts_dt(meta.get("creationTimestamp")),
        started=parse_ts_dt(status.get("startTime")),
        finally_start=parse_ts_dt(status.get("finallyStartTime")),
        completed=parse_ts_dt(status.get("completionTime")),
        succeeded_status=condition.get("status"),
        succeeded_reason=condition.get("reason"),
        succeeded_message=condition.get("message"),
        tr_refs=refs,
        skipped_stopping=skipped,
    )


def parse_taskrun(raw: dict, namespace: str | None = None) -> TaskRunRecord:
    """Build a TaskRunRecord from a raw TaskRun manifest."""
    meta = raw.get("metadata") or {}
    status = raw.get("status") or {}
    condition = succeeded_condition(raw)
    steps = []
    for step in status.get("steps") or []:
        terminated = step.get("terminated") or {}
        steps.append(
            StepRecord(
                name=step.get("name"),
                container=step.get("container"),
                started_at=parse_ts_dt(terminated.get("startedAt")),
                finished_at=parse_ts_dt(terminated.get("finishedAt")),
                reason=terminated.get("reason"),
                exit_code=terminated.get("exitCode"),
                message=terminated.get("message"),
            )
        )
    sidecars = [s.get("container") for s in status.get("sidecars") or [] if s.get("container")]
    return TaskRunRecord(
        name=meta.get("name") or "unknown",
        namespace=meta.get("namespace") or namespace or "",
        pipeline_task=(meta.get("labels") or {}).get("tekton.dev/pipelineTask"),
        created=parse_ts_dt(meta.get("creationTimestamp")),
        started=parse_ts_dt(status.get("startTime")),
        completed=parse_ts_dt(status.get("completionTime")),
        succeeded_status=condition.get("status"),
        succeeded_reason=condition.get("reason"),
        succeeded_message=condition.get("message"),
        pod_name=status.get("podName"),
        steps=steps,
        sidecars=sidecars,
    )


def parse_pod(raw: dict, namespace: str | None = None) -> PodRecord:
    """Build a PodRecord from a raw Pod manifest."""
    meta = raw.get("metadata") or {}
    status = raw.get("status") or {}
    conditions = [
        (
            c.get("type"),
            c.get("status"),
            c.get("reason"),
            c.get("lastTransitionTime"),
        )
        for c in status.get("conditions") or []
    ]
    containers = []
    for container in (status.get("initContainerStatuses") or []) + (
        status.get("containerStatuses") or []
    ):
        state = container.get("state") or {}
        terminated = state.get("terminated") or {}
        waiting = state.get("waiting") or {}
        containers.append(
            ContainerStatusRecord(
                name=container.get("name"),
                terminated_reason=terminated.get("reason"),
                terminated_started=parse_ts_dt(terminated.get("startedAt")),
                terminated_finished=parse_ts_dt(terminated.get("finishedAt")),
                terminated_exit_code=terminated.get("exitCode"),
                waiting_reason=waiting.get("reason"),
            )
        )
    return PodRecord(
        name=meta.get("name") or "unknown",
        namespace=meta.get("namespace") or namespace or "",
        node=(raw.get("spec") or {}).get("nodeName"),
        phase=status.get("phase"),
        conditions=conditions,
        containers=containers,
    )


# ---------------------------------------------------------------------------
# 5. Cluster: direct API access loaded from kubeconfig
# ---------------------------------------------------------------------------

# api_version candidates are tried in order: the live cluster and KubeArchive
# both serve tekton as v1 (KubeArchive's aggregated tekton.dev/v1 returns the
# archived object; v1beta1 404s for it), so v1 is tried first with v1beta1 as
# a fallback for older servers.  The first api_version that resolves and
# returns a hit wins.
KIND_API = {
    "pipelinerun": (("tekton.dev/v1", "tekton.dev/v1beta1"), "PipelineRun"),
    "taskrun": (("tekton.dev/v1", "tekton.dev/v1beta1"), "TaskRun"),
    "pod": (("v1",), "Pod"),
}


def _kubeconfig_paths() -> list[str]:
    """Return the kubeconfig file paths to consider (KUBECONFIG first)."""
    env = os.environ.get("KUBECONFIG")
    if env:
        return [path for path in env.split(os.pathsep) if path]
    return [str(Path.home() / ".kube" / "config")]


def _current_cluster() -> tuple[str | None, str | None]:
    """Return (cluster_name, server_url) of the current kubeconfig context."""
    for path in _kubeconfig_paths():
        try:
            data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001 - tolerate unreadable kubeconfigs
            logger.debug("could not read kubeconfig %s: %s", path, exc)
            continue
        if not isinstance(data, dict):
            continue
        clusters = {
            (cluster.get("name") or ""): (cluster.get("cluster") or {})
            for cluster in data.get("clusters") or []
        }
        current = data.get("current-context")
        for context in data.get("contexts") or []:
            if context.get("name") == current:
                cluster_name = (context.get("context") or {}).get("cluster")
                server = (clusters.get(cluster_name) or {}).get("server")
                return cluster_name, server
    return None, None


def _current_cluster_name() -> str | None:
    """Return the cluster name of the current kubeconfig context, or None."""
    return _current_cluster()[0]


def _normalize_url(url: object) -> str | None:
    """Canonical form of a server URL for comparison (host:port, lowercased)."""
    if not url:
        return None
    text = str(url).strip()
    for scheme in ("https://", "http://"):
        text = text.removeprefix(scheme)
    text = text.rstrip("/").lower()
    return text or None


DEFAULT_KA_CONF = Path.home() / ".config" / "kubectl-ka.conf"
DEFAULT_KA_CONF_ENV = "KUBECTL_KA_CONFIG"


def load_ka_conf(path: str) -> dict:
    """Load a kubectl-ka.conf file -> {cluster_name: {server_url, host}}."""
    try:
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    except Exception as exc:
        raise ClusterError(f"cannot read KubeArchive config {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ClusterError(f"{path}: expected a mapping at top level")
    clusters = data.get("clusters")
    if not isinstance(clusters, dict):
        raise ClusterError(f"{path}: missing 'clusters' mapping")
    return clusters


def ka_host_for_cluster(
    clusters: dict,
    cluster_name: str | None = None,
    server_url: str | None = None,
) -> str | None:
    """KubeArchive API host for a cluster, or None.

    Matches by kubeconfig cluster name first (exact), then by API server URL
    (normalized).  Kubeconfig cluster names are often OCP-normalized
    (dots->dashes, with port) while kubectl-ka.conf keys use the raw server
    hostname, so server_url matching is the reliable fallback.
    """
    if cluster_name:
        entry = clusters.get(cluster_name)
        if isinstance(entry, dict):
            return entry.get("host")
    if server_url:
        want = _normalize_url(server_url)
        if want:
            for entry in clusters.values():
                if isinstance(entry, dict) and _normalize_url(entry.get("server_url")) == want:
                    return entry.get("host")
    return None


def _ka_conf_host(ka_conf: str | None = None) -> str | None:
    """Resolve the KubeArchive API host for the current cluster.

    Reads a kubectl-ka.conf (--ka-conf, $KUBECTL_KA_CONFIG, or
    ~/.config/kubectl-ka.conf) and looks up the cluster of the current
    kubeconfig context.  Returns None when no KA endpoint is configured.
    """
    path = ka_conf or os.environ.get(DEFAULT_KA_CONF_ENV) or str(DEFAULT_KA_CONF)
    if not Path(path).is_file():
        logger.debug("no KubeArchive config file: %s", path)
        return None
    clusters = load_ka_conf(path)
    cluster_name, server = _current_cluster()
    host = ka_host_for_cluster(clusters, cluster_name, server)
    if host is None:
        logger.warning(
            "no KubeArchive host for cluster %r (server %r) in %s",
            cluster_name,
            server,
            path,
        )
        return None
    logger.debug("KubeArchive host for cluster %r: %s", cluster_name, host)
    return host


def _find_ka_context() -> str | None:
    """Best-effort: find a kubeconfig context pointing at KubeArchive.

    Looks for a context whose name contains 'kubearchive' or whose cluster
    server URL does.  Returns None when none is found (fallback disabled).
    """
    for path in _kubeconfig_paths():
        try:
            data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001 - tolerate unreadable kubeconfigs
            logger.debug("could not read kubeconfig %s: %s", path, exc)
            continue
        if not isinstance(data, dict):
            continue
        clusters = {
            (cluster.get("name") or ""): (cluster.get("cluster") or {})
            for cluster in data.get("clusters") or []
        }
        for context in data.get("contexts") or []:
            context_meta = context.get("context") or {}
            cluster_name = context_meta.get("cluster")
            server = (clusters.get(cluster_name) or {}).get("server") or ""
            candidate = context.get("name") or ""
            if "kubearchive" in candidate.lower() or "kubearchive" in (server or "").lower():
                return candidate
    return None


def _build_api_client(context: str | None):
    """Build a Kubernetes DynamicClient from a kubeconfig context (or current)."""
    from kubernetes import client as kubernetes_client
    from kubernetes import config as kubernetes_config
    from kubernetes.dynamic import DynamicClient

    configuration = kubernetes_client.Configuration()
    try:
        kubernetes_config.load_kube_config(client_configuration=configuration, context=context)
    except Exception as exc:
        raise ClusterError(f"cannot load kubeconfig (context={context!r}): {exc}") from exc
    return DynamicClient(kubernetes_client.ApiClient(configuration))


def _build_ka_client_from_host(main_cfg, host: str):
    """Build a DynamicClient for a KubeArchive API host.

    Reuses the authentication (bearer token / certs) from the main cluster
    configuration; KubeArchive's apiserver authenticates via TokenReview
    against the cluster it fronts, so the same token works.

    A plain copy of the main configuration is NOT safe here: load_kube_config
    installs refresh_api_key_hook on it, and that hook re-applies the whole
    cluster context (host, TLS, token) onto the configuration, so the first
    request would re-stamp the live cluster's host over our KubeArchive host
    (silently turning the "fallback" into a second query against the live
    cluster).  We copy the auth/TLS fields we actually need instead.
    """
    from kubernetes import client as kubernetes_client
    from kubernetes.dynamic import DynamicClient

    ka_cfg = kubernetes_client.Configuration()
    ka_cfg.host = host if host.startswith(("http://", "https://")) else f"https://{host}"
    for attr in (
        "api_key",
        "api_key_prefix",
        "ssl_ca_cert",
        "cert_file",
        "key_file",
        "verify_ssl",
        "assert_hostname",
        "tls_server_name",
        "proxy",
        "no_proxy",
    ):
        if hasattr(main_cfg, attr):
            try:
                setattr(ka_cfg, attr, getattr(main_cfg, attr))
            except Exception:  # noqa: BLE001 - tolerate odd auth setups
                pass
    return DynamicClient(kubernetes_client.ApiClient(ka_cfg))


def _to_plain_dict(obj) -> dict:
    """Convert a Kubernetes client response object into a plain dict."""
    to_dict = getattr(obj, "to_dict", None)
    if callable(to_dict):
        return to_dict()
    if isinstance(obj, dict):
        return obj
    return dict(obj)


class Cluster:
    """Direct Kubernetes API access (no `oc` subprocesses).

    Kinds are fetched from the live cluster API first; when that fails the
    client falls back to KubeArchive.  KubeArchive is resolved in order of:
    1. an explicit kubeconfig context (--ka-context)
    2. a kubectl-ka.conf endpoint for the current cluster (--ka-conf)
    3. an auto-detected kubeconfig context whose name/server host it
       (kubearchive)
    """

    def __init__(self, ka_context: str | None = None, ka_conf: str | None = None):
        self.ka_context = ka_context
        self.ka_conf = ka_conf
        self.dyn = _build_api_client(None)
        self.ka_dyn = self._resolve_ka_client()
        from kubernetes import client as kubernetes_client

        self.core = kubernetes_client.CoreV1Api(self.dyn.client)

    def _resolve_ka_client(self):
        """Build the KubeArchive client (or None when none is configured)."""
        if self.ka_context:
            try:
                client = _build_api_client(self.ka_context)
            except ClusterError as exc:
                raise ClusterError(
                    f"requested KubeArchive context not in kubeconfig: {self.ka_context}"
                ) from exc
            logger.info("using KubeArchive context: %s", self.ka_context)
            return client
        host = _ka_conf_host(self.ka_conf)
        if host:
            logger.info("using KubeArchive API server: %s", host)
            return _build_ka_client_from_host(self.dyn.client.configuration, host)
        context = _find_ka_context()
        if context:
            logger.info("using KubeArchive context (auto): %s", context)
            return _build_api_client(context)
        return None

    @property
    def _candidates(self) -> Iterator:
        """Yield (client, label) pairs: live cluster then KubeArchive."""
        yield self.dyn, "cluster"
        if self.ka_dyn is not None:
            yield self.ka_dyn, "kubearchive"

    @staticmethod
    def _get_from(client, kind: str, namespace: str, name: str) -> dict:
        from kubernetes.client.rest import ApiException

        api_versions, kubernetes_kind = KIND_API[kind]
        last_error: Exception | None = None
        for api_version in api_versions:
            try:
                resource = client.resources.get(api_version=api_version, kind=kubernetes_kind)
            except Exception as exc:  # noqa: BLE001 - version not in discovery
                last_error = exc
                continue
            try:
                instance = client.get(resource, namespace=namespace, name=name)
                return _to_plain_dict(instance)
            except ApiException as exc:
                # Try the next api_version candidate.
                last_error = exc
                continue
        if last_error is not None:
            raise last_error
        raise ClusterError(f"no api_version available for {kind}")

    def get_object(self, kind: str, namespace: str, name: str) -> dict | None:
        """Fetch one object as a plain dict; None when not found anywhere.

        Raises ClusterError only when every backend failed with a non-404
        error (network/auth/API problem) - that is a fail-fast condition.
        """
        from kubernetes.client.rest import ApiException

        errors: list[str] = []
        not_found = False
        for client, label in self._candidates:
            try:
                return self._get_from(client, kind, namespace, name)
            except ApiException as exc:
                if exc.status == 404:
                    not_found = True
                    body = str(getattr(exc, "body", None) or "").strip()
                    detail = f": {body[:300]}" if body else ""
                    logger.debug(
                        "%s: %s %s/%s: not found%s",
                        label,
                        kind,
                        namespace,
                        name,
                        detail,
                    )
                    continue
                errors.append(f"{label}: HTTP {exc.status} {exc.reason}")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{label}: {exc}")
                continue
        if not_found:
            return None
        raise ClusterError(
            f"could not fetch {kind} {namespace}/{name}: "
            + ("; ".join(errors) if errors else "no backend available")
        )

    def get_logs(self, namespace: str, pod: str, container: str) -> str | None:
        """Return a container's logs, or None when unavailable (best-effort)."""
        from kubernetes.client.rest import ApiException

        try:
            return self.core.read_namespaced_pod_log(namespace, pod, container=container)
        except ApiException as exc:
            logger.debug("no logs for %s/%s %s: HTTP %d", namespace, pod, container, exc.status)
            return None
        except Exception as exc:  # noqa: BLE001
            logger.debug("no logs for %s/%s %s: %s", namespace, pod, container, exc)
            return None

    def wait_completed(
        self, namespace: str, name: str, timeout: float, interval: float = POLL_INTERVAL
    ) -> dict | None:
        """Poll until status.completionTime is set; None on timeout."""
        deadline = time.monotonic() + timeout
        while True:
            obj = self.get_object("pipelinerun", namespace, name)
            if obj is not None and (obj.get("status") or {}).get("completionTime"):
                return obj
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            time.sleep(min(interval, remaining))


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


# ---------------------------------------------------------------------------
# 7. download + wait operations
# ---------------------------------------------------------------------------


def collect_plr_manifest(
    store: CacheStore,
    cluster: Cluster,
    target: Target,
    also_incomplete: bool = False,
) -> tuple[str, PLRRecord | None]:
    """Fetch and cache one PLR manifest.

    Cached files win (fetch-or-cached.sh semantics): when the PLR is already in
    the cache it is used without touching the cluster.  Returns ("dumped",
    record) when the PLR was cached, ("missing", None) when it does not exist
    anywhere, or ("incomplete", None) when it is still running and
    --also-incomplete was not set.  Raises ClusterError on API problems.
    """
    raw = store.read_cached_object("pipelinerun", target.plr)
    if raw is None:
        raw = cluster.get_object("pipelinerun", target.namespace, target.plr)
    else:
        logger.debug("using cached PipelineRun %s/%s", target.namespace, target.plr)
    if raw is None:
        logger.warning(
            "%s/%s: PipelineRun not found in cache, cluster or KubeArchive",
            target.namespace,
            target.plr,
        )
        return "missing", None
    parsed = parse_plr(raw, target.namespace)
    if parsed.succeeded_status not in ("True", "False") and not also_incomplete:
        logger.debug(
            "%s/%s: cached copy still running; use --also-incomplete to cache it anyway",
            target.namespace,
            target.plr,
        )
        return "incomplete", None
    record = store.add_plr(raw, target.namespace)
    return "dumped", record


def _fetch_details(store: CacheStore, cluster: Cluster, plr: PLRRecord, target: Target) -> None:
    """Fetch TaskRuns, Pods and container logs for a PLR and link the records.

    Already-cached TaskRun/Pod/log files are reused instead of re-fetching
    (same cached-wins policy as collect_plr_manifest).
    """
    for tr_name in plr.tr_refs:
        raw_tr = store.read_cached_object("taskrun", tr_name)
        if raw_tr is None:
            raw_tr = cluster.get_object("taskrun", target.namespace, tr_name)
        if raw_tr is None:
            logger.warning("TaskRun %s/%s not found; skipping", target.namespace, tr_name)
            continue
        tr = store.add_taskrun(raw_tr, target.namespace)
        plr.taskruns.append(tr)
        pod_name = tr.pod_name
        if not pod_name:
            continue
        raw_pod = store.read_cached_object("pod", pod_name)
        if raw_pod is None:
            raw_pod = cluster.get_object("pod", target.namespace, pod_name)
        if raw_pod is None:
            logger.warning("Pod %s/%s not found; skipping", target.namespace, pod_name)
        else:
            tr.pod = store.add_pod(raw_pod, target.namespace)
        for container in *(step.container for step in tr.steps if step.container), *tr.sidecars:
            text = store.read_cached_log(pod_name, container)
            if text is None:
                text = cluster.get_logs(target.namespace, pod_name, container)
            if text is not None:
                store.add_log(pod_name, container, text)


def collect_one(
    store: CacheStore, cluster: Cluster, options: DownloadOptions, target: Target
) -> bool:
    """Collect one PLR (and optionally its details). True on success.

    Not-found PLRs return False (counted as failures at the end); cluster-level
    errors propagate so the caller can retry.
    """
    result, record = collect_plr_manifest(
        store, cluster, target, also_incomplete=options.also_incomplete
    )
    if result == "missing":
        # collect_plr_manifest already logged the detailed warning.
        return False
    if result == "incomplete":
        logger.info(
            "%s/%s: still running, skipped (use --also-incomplete to cache it anyway)",
            target.namespace,
            target.plr,
        )
        return True
    assert record is not None
    if options.details_included or (
        options.details_if_failed and record.succeeded_status == "False"
    ):
        _fetch_details(store, cluster, record, target)
    return True


def _collect_with_retry(
    store: CacheStore, cluster: Cluster, options: DownloadOptions, target: Target
) -> bool:
    """collect_one with the retry-until-success policy from collect-plrs.sh."""
    last_error: Exception | None = None
    for attempt in range(1, options.retries + 1):
        try:
            return collect_one(store, cluster, options, target)
        except ClusterError as exc:
            last_error = exc
            logger.warning(
                "attempt %d/%d failed for %s/%s: %s",
                attempt,
                options.retries,
                target.namespace,
                target.plr,
                exc,
            )
            if attempt < options.retries:
                time.sleep(options.retry_sleep)
    assert last_error is not None
    raise last_error


def run_download(
    store: CacheStore, cluster: Cluster, options: DownloadOptions, targets: list[Target]
) -> int:
    """Run the whole download subcommand. Returns process exit code."""
    failed = 0
    with ThreadPoolExecutor(max_workers=options.concurrency) as pool:
        futures = {
            pool.submit(_collect_with_retry, store, cluster, options, target): target
            for target in targets
        }
        for future in as_completed(futures):
            target = futures[future]
            try:
                if not future.result():
                    failed += 1
            except Exception as exc:  # noqa: BLE001
                logger.error("%s/%s: collection failed: %s", target.namespace, target.plr, exc)
                failed += 1
    logger.info("download: %d/%d PLRs collected", len(targets) - failed, len(targets))
    return 1 if failed else 0


def cmd_download(args: argparse.Namespace) -> int:
    """CLI entry for 'download'."""
    options = DownloadOptions(
        cache_dir=Path(args.cache),
        concurrency=args.concurrency,
        also_incomplete=args.also_incomplete,
        details_if_failed=args.details_if_failed,
        details_included=args.details_included,
        with_timing=(
            TimingOptions(gantt_chart=args.gantt_chart, summary=args.summary)
            if args.with_timing
            else None
        ),
        with_errors=args.with_errors,
        ka_context=args.ka_context,
        ka_conf=args.ka_conf,
    )
    targets = resolve_targets(args)
    cluster = Cluster(ka_context=options.ka_context, ka_conf=options.ka_conf)
    store = CacheStore(options.cache_dir)
    exit_code = run_download(store, cluster, options, targets)
    if options.with_timing:
        run_timing(store, options.with_timing)
    if options.with_errors:
        run_errors(store)
    return exit_code


def run_wait(cluster: Cluster, options: WaitOptions, targets: list[Target]) -> int:
    """Run the whole wait subcommand. Returns process exit code."""
    if not targets:
        return 0
    failed = 0
    # Canary first (original behavior): a broken cluster fails fast on one PLR.
    canary = targets[-1]
    logger.info(
        "waiting for canary %s/%s (timeout %.0fs)", canary.namespace, canary.plr, options.timeout
    )
    if cluster.wait_completed(canary.namespace, canary.plr, options.timeout) is None:
        logger.error(
            "canary %s/%s did not complete within %.0fs",
            canary.namespace,
            canary.plr,
            options.timeout,
        )
        return 1
    logger.info("canary %s/%s completed", canary.namespace, canary.plr)
    rest = targets[:-1]
    with ThreadPoolExecutor(max_workers=options.concurrency) as pool:
        futures = {
            pool.submit(cluster.wait_completed, t.namespace, t.plr, options.timeout): t
            for t in rest
        }
        for future in as_completed(futures):
            target = futures[future]
            if future.result() is None:
                logger.error(
                    "%s/%s did not complete within %.0fs",
                    target.namespace,
                    target.plr,
                    options.timeout,
                )
                failed += 1
    print(f"wait: {len(targets)} PLRs, {len(targets) - failed} completed")
    if options.dump_completed:
        store = CacheStore(options.cache_dir)
        _dump_completed_plrs(store, cluster, targets)
    return 1 if failed else 0


def _dump_completed_plrs(store: CacheStore, cluster: Cluster, targets: list[Target]) -> None:
    """Shared download path reused by 'wait --dump-completed'."""
    for target in targets:
        result, _record = collect_plr_manifest(store, cluster, target)
        if result == "missing":
            logger.warning("%s/%s: not found while dumping", target.namespace, target.plr)


def cmd_wait(args: argparse.Namespace) -> int:
    """CLI entry for 'wait'."""
    options = WaitOptions(
        cache_dir=Path(args.cache),
        concurrency=args.concurrency,
        timeout=parse_duration(args.timeout),
        dump_completed=args.dump_completed,
    )
    targets = resolve_targets(args)
    cluster = Cluster()
    return run_wait(cluster, options, targets)


# ---------------------------------------------------------------------------
# 8. timing analysis
# ---------------------------------------------------------------------------


def _print_duration_stats(label: str, values: list) -> None:
    """Print a min/avg/p99/max duration line (original summary format)."""
    if not values:
        print(f"  {label:<20} min={'n/a':<6} avg={'n/a':<6} p99={'n/a':<6} max={'n/a':<6}")
        return
    p9999 = percentile(values)
    print(
        f"  {label:<20} min={min(values)}s avg={sum(values)/len(values):.1f}s "
        f"p99={p9999}s max={max(values)}s"
    )


def _print_ts_range(label: str, values: list) -> None:
    """Print a min/max timestamp line (original summary format)."""
    if not values:
        print(f"  {label:<20} min={'n/a':<22} max={'n/a':<22}")
        return
    fmt = lambda value: dt.datetime.fromtimestamp(value, tz=dt.UTC).strftime(TD_FMT)
    print(f"  {label:<20} min={fmt(min(values)):<22} max={fmt(max(values)):<22}")


def _ts_stat(values: list) -> dict:
    """Build the timestamp YAML/JSON stats dict (epoch ints)."""
    if not values:
        return {"min": None, "max": None, "data": []}
    return {"min": min(values), "max": max(values), "data": values}


def _duration_stat(values: list) -> dict:
    """Build a duration stats dict (seconds, p99 nearest-rank)."""
    if not values:
        return {"min": None, "avg": None, "p99": None, "max": None, "data": []}
    return {
        "min": min(values),
        "avg": round(sum(values) / len(values), 1),
        "p99": percentile(values),
        "max": max(values),
        "data": values,
    }


class _Gantt:
    """Collects (label, start, end, color) rows and renders a matplotlib chart."""

    # 0 = pending (blue), 1 = running (red) - same colors as the gnuplot chart.
    COLORS = ("#0000FF", "#FF0000")

    def __init__(self) -> None:
        self.rows: list[tuple[str, dt.datetime, dt.datetime, int]] = []

    def add(
        self,
        label: str,
        start: dt.datetime | None,
        end: dt.datetime | None,
        color: int,
    ) -> None:
        """Register one horizontal bar (skipped when either timestamp is missing)."""
        if start is None or end is None:
            return
        if end < start:
            start, end = end, start
        self.rows.append((label, start, end, color))

    def render(self, output: str) -> None:
        """Render the Gantt chart to the given PNG path."""
        if not self.rows:
            print("Skipped generating Gantt diagram because data were missing")
            return
        import matplotlib
        import matplotlib.pyplot as plt
        from matplotlib import dates as mdates

        matplotlib.use("Agg")
        fig = plt.figure(figsize=(12, min(max(len(self.rows) * 0.4, 4.0), 40.0)), dpi=100)
        ax = fig.add_subplot(111)
        numbers = []
        for label, start, end, color in self.rows:
            x0 = mdates.date2num(start)
            x1 = mdates.date2num(end)
            numbers.append((label, x0, x1, color))
        x_values = [num for _l, x0, x1, _c in numbers for num in (x0, x1)]
        x_min, x_max = min(x_values), max(x_values)
        padding = max((x_max - x_min) * 0.02, 60.0 / 86400.0)
        for y, (label, x0, x1, color) in enumerate(numbers):
            ax.barh(y, x1 - x0, left=x0, height=0.6, color=self.COLORS[color])
        ax.set_xlim(x_min - padding, x_max + padding)
        ax.invert_yaxis()
        ax.set_yticks(range(len(numbers)))
        ax.set_yticklabels([label for label, _x0, _x1, _c in numbers], fontsize=8)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        ax.grid(True, axis="x", alpha=0.3)
        fig.tight_layout()
        fig.savefig(output)
        plt.close(fig)
        print(f"Generated Gantt diagram to {BLUE}{output}{RESET}")


def run_timing(store: CacheStore, options: TimingOptions) -> int:
    """Timing analysis over cached *succeeded* PLRs (cache only, no cluster)."""
    plrs = sorted(
        (rec for rec in store.plrs.values() if rec.succeeded_status == "True"),
        key=lambda rec: (
            rec.created is None,
            rec.created or dt.datetime.min.replace(tzinfo=dt.UTC),
        ),
    )
    if not plrs:
        logger.warning("no succeeded PipelineRuns found in %s", store.path)
        return 0

    created_epochs: list[int] = []
    started_epochs: list[int] = []
    completed_epochs: list[int] = []
    pending_durations: list[int] = []
    running_durations: list[int] = []
    total_durations: list[int] = []
    stat_waiting_time = 0
    gantt = _Gantt()

    for rec in plrs:
        _print_plr_header(rec)
        pr_created = epoch_of(rec.created)
        pr_started = epoch_of(rec.started)
        pr_completed = epoch_of(rec.completed)
        for value, bucket in (
            (pr_created, created_epochs),
            (pr_started, started_epochs),
            (pr_completed, completed_epochs),
        ):
            if value is not None:
                bucket.append(value)
        pr_pending = duration_seconds(rec.created, rec.started)
        pr_running = duration_seconds(rec.started, rec.completed)
        pr_total = duration_seconds(rec.created, rec.completed)
        for value, bucket in (
            (pr_pending, pending_durations),
            (pr_running, running_durations),
            (pr_total, total_durations),
        ):
            if value is not None:
                bucket.append(value)

        gantt.add(f"PLR {rec.name}", rec.created, rec.completed, 0)
        if rec.started is not None:
            gantt.add(f"PLR {rec.name} run", rec.started, rec.completed, 1)

        trs_earliest: dt.datetime | None = None
        for tr in sorted(
            rec.taskruns, key=lambda item: (item.created or dt.datetime.min.replace(tzinfo=dt.UTC))
        ):
            if tr.created is not None and (trs_earliest is None or tr.created < trs_earliest):
                trs_earliest = tr.created
            _print_tr(tr)
            gantt.add(f"TR {tr.pipeline_task}", tr.created, tr.completed, 0)
            if tr.started is not None:
                gantt.add(f"TR {tr.pipeline_task} run", tr.started, tr.completed, 1)
            if tr.steps:
                print(f"     {YELLOW}Steps:{RESET}")
                first_step: dt.datetime | None = None
                for step in tr.steps:
                    duration = duration_seconds(step.started_at, step.finished_at)
                    reason = step.reason or "n/a"
                    print(
                        f"       {step.name!s:<35} {('n/a' if duration is None else str(duration) + 's'):>5}  {reason}"
                    )
                    gantt.add(f"Step {step.name}", step.started_at, step.finished_at, 1)
                    if step.started_at is not None and (
                        first_step is None or step.started_at < first_step
                    ):
                        first_step = step.started_at
                if first_step is not None and tr.created is not None:
                    tr_wait = int((first_step - tr.created).total_seconds())
                    stat_waiting_time += tr_wait
                    print(
                        f"     {YELLOW}TaskRun wait time (creation to first step): {tr_wait}s{RESET}"
                    )
            if tr.pod is not None:
                _print_pod(tr.pod)
            print()
        if rec.created is not None and trs_earliest is not None:
            plr_wait = int((trs_earliest - rec.created).total_seconds())
            stat_waiting_time += plr_wait
            print(
                f" ⤷ {YELLOW}PipelineRun wait time (creation to first TaskRun): {plr_wait}s{RESET}"
            )
        print()

    if options.gantt_chart:
        gantt.render(options.gantt_chart)

    print(f"Total waiting time: {BLUE}{stat_waiting_time} seconds{RESET}")
    print()
    print(f"{YELLOW}=== Summary ({len(plrs)} succeeded PipelineRuns) ==={RESET}")
    _print_ts_range("creationTimestamp", created_epochs)
    _print_ts_range("startTime", started_epochs)
    _print_ts_range("completionTime", completed_epochs)
    _print_duration_stats("pending", pending_durations)
    _print_duration_stats("running", running_durations)
    _print_duration_stats("total", total_durations)

    if options.summary:
        _write_summary(
            options.summary,
            created_epochs,
            started_epochs,
            completed_epochs,
            pending_durations,
            running_durations,
            total_durations,
            len(plrs),
        )
    return 0


def _print_plr_header(rec: PLRRecord) -> None:
    """Print the per-PLR timing header line (original format)."""
    pending = duration_seconds(rec.created, rec.started)
    total = duration_seconds(rec.created, rec.completed)
    running = duration_seconds(rec.started, rec.completed)
    print(f"{YELLOW}PipelineRun '{rec.name}' ({rec.pipeline}) duration:{RESET}")
    header = (
        f"  {'creationTimestamp':<22} {'startTime':<22} {'completionTime':<22}"
        f" pending={'n/a' if pending is None else pending} "
        f"total={'n/a' if total is None else total} "
        f"running={'n/a' if running is None else running}"
    )
    print(header.rstrip())
    print(f"  {fmt_ts(rec.created):<22} {fmt_ts(rec.started):<22} {fmt_ts(rec.completed):<22}")


def _print_tr(tr: TaskRunRecord) -> None:
    """Print the per-TaskRun timing lines (original format)."""
    print(f" ⤷ {YELLOW}TaskRun '{tr.pipeline_task}' ({tr.name}):{RESET}")
    pending = duration_seconds(tr.created, tr.started)
    running = duration_seconds(tr.started, tr.completed)
    total = duration_seconds(tr.created, tr.completed)
    print(
        f"     {fmt_ts(tr.created):<22} {fmt_ts(tr.started):<22} {fmt_ts(tr.completed):<22} "
        f"pending={'n/a' if pending is None else pending}s "
        f"running={'n/a' if running is None else running}s "
        f"total={'n/a' if total is None else total}s"
    )


def _print_pod(pod: PodRecord) -> None:
    """Print the pod node, conditions and container lines (original format)."""
    print(f"     {YELLOW}Pod '{pod.name}' on node {BLUE}{pod.node or 'null'}{RESET}")
    for ctype, cstatus, creason, ctime in pod.conditions:
        print(
            f"         {ctype!s:<20} {cstatus or '-'!s:<6} "
            f"{creason or '-'!s:<25} {ctime or '-'}"
        )
    for container in pod.containers:
        duration = duration_seconds(container.terminated_started, container.terminated_finished)
        duration_text = "n/a" if duration is None else f"{duration}s"
        reason = container.terminated_reason or "n/a"
        print(f"         {container.name:<35} {duration_text:>5}  {reason}")


def _write_summary(
    path: str,
    created: list,
    started: list,
    completed: list,
    pending: list,
    running: list,
    total: list,
    succeeded_count: int,
) -> None:
    """Write aggregate timing stats as JSON (mirrors the old timings.yaml)."""
    document = {
        "creationTimestamp": _ts_stat(created),
        "startTime": _ts_stat(started),
        "completionTime": _ts_stat(completed),
        "pending": _duration_stat(pending),
        "running": _duration_stat(running),
        "total": _duration_stat(total),
        "Succeeded": {"total": succeeded_count, "True": succeeded_count},
    }
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(document, indent=1) + "\n", encoding="utf-8")
    print(f"Wrote summary stats to {BLUE}{target}{RESET}")


def cmd_timing(args: argparse.Namespace) -> int:
    """CLI entry for 'timing'."""
    store = CacheStore(Path(args.cache)).load()
    options = TimingOptions(gantt_chart=args.gantt_chart, summary=args.summary)
    return run_timing(store, options)


# ---------------------------------------------------------------------------
# 9. errors analysis
# ---------------------------------------------------------------------------


def collect_plr_conditions(plrs) -> Counter:
    """Count (status, reason, normalized message) across PipelineRuns."""
    histogram = Counter()
    for rec in plrs:
        if rec.succeeded_status == "True" and rec.succeeded_reason == "Succeeded":
            continue
        histogram[
            (
                rec.succeeded_status or MISSING,
                rec.succeeded_reason or MISSING,
                normalize_message(rec.succeeded_message),
            )
        ] += 1
    return histogram


def collect_taskrun_conditions(taskruns) -> defaultdict:
    """Count (status, reason, message) per pipelineTask name across TaskRuns."""
    histograms = defaultdict(Counter)
    for tr in taskruns:
        if tr.succeeded_status == "True" and tr.succeeded_reason == "Succeeded":
            continue
        histograms[tr.pipeline_task or MISSING][
            (
                tr.succeeded_status or MISSING,
                tr.succeeded_reason or MISSING,
                normalize_message(tr.succeeded_message),
            )
        ] += 1
    return histograms


def collect_pod_data(pods):
    """Return (phases, terminated-reasons, waiting-reasons, false-conditions)."""
    phases = Counter()
    terminated = defaultdict(Counter)
    waiting = defaultdict(Counter)
    false_conditions = Counter()
    for pod in pods:
        phases[pod.phase or MISSING] += 1
        for container in pod.containers:
            if container.terminated_reason and container.terminated_reason != "Completed":
                terminated[container.name][container.terminated_reason] += 1
            if container.waiting_reason:
                waiting[container.name][container.waiting_reason] += 1
        for ctype, cstatus, creason, _ctime in pod.conditions:
            if cstatus == "False":
                false_conditions[(ctype, creason or MISSING)] += 1
    return phases, terminated, waiting, false_conditions


def _print_histogram(title: str, histogram: Counter) -> None:
    """Print a single histogram sorted by count, most frequent first."""
    print(f"=== {title} ===")
    if not histogram:
        print("  (no data)")
        print()
        return
    for key, count in sorted(histogram.items(), key=lambda kv: kv[1], reverse=True):
        if isinstance(key, tuple):
            key_str = " | ".join(str(part) for part in key)
        else:
            key_str = str(key)
        print(f"  {count:4d}  {key_str}")
    print()


def _print_grouped_histograms(title: str, histograms_by_group: defaultdict) -> None:
    """Print histograms for all groups combined, sorted by count."""
    print(f"=== {title} ===")
    rows = [
        (count, group, key)
        for group, histogram in histograms_by_group.items()
        for key, count in histogram.items()
    ]
    if not rows:
        print("  (no data)")
        print()
        return
    for count, group, key in sorted(rows, key=lambda row: row[0], reverse=True):
        if isinstance(key, tuple):
            key_str = " | ".join(str(part) for part in key)
        else:
            key_str = str(key)
        print(f"  {count:4d}  {group}: {key_str}")
    print()


def _report_failed_taskrun(tr: TaskRunRecord) -> None:
    """Investigate one failed TaskRun (check-plr-errors.sh logic)."""
    reason = tr.succeeded_reason
    message = tr.succeeded_message or ""
    if reason == "TaskRunImagePullFailed":
        print(f" ⤷ {BLUE}TaskRun image pull failed: {tr.pipeline_task} ({tr.name}){RESET}")
        print(f"   {message}")
        return
    if reason == "PodCreationFailed":
        print(f" ⤷ {BLUE}TaskRun pod creation failed: {tr.pipeline_task} ({tr.name}){RESET}")
        print(f"   {message}")
        return
    if "OOMKilled" in message:
        print(f" ⤷ {BLUE}TaskRun OOMKilled: {tr.pipeline_task} ({tr.name}){RESET}")
        print(f"   {message}")
        return
    if reason == "TaskRunCancelled":
        print(f" ⤷ {BLUE}TaskRun cancelled: {tr.pipeline_task} ({tr.name}){RESET}")
        print(f"   {message}")
        return
    print(f" ⤷ {RED}TaskRun failed: {tr.pipeline_task} ({tr.name}){RESET}")
    print(f"   Reason: {tr.succeeded_reason}")
    print(f"   Message: {message}")
    for step in tr.steps:
        if step.exit_code is None or step.exit_code == 0:
            continue
        step_message = step.message or ""
        if "Skipping step because a previous step failed" in step_message:
            print(f"      ⤷ {BLUE}Step skipped: {step.name} (prior step failed){RESET}")
            continue
        if (
            step.name == "step-assert"
            and step.exit_code == 1
            and re.search(r"key.*TEST_OUTPUT.*result.*FAILURE", step_message)
        ):
            print(f"      ⤷ {BLUE}Enterprise Contract assert failed: {step.name}{RESET}")
            print(f"        {step_message[:500]}")
            print()
            continue
        print(
            f"      ⤷ {RED}Step failed: {step.name} (exit code: {step.exit_code}, "
            f"reason: {step.reason or 'unknown'}){RESET}"
        )
        if step_message:
            print(f"        Message: {step_message[:1000]}")
            print()


def classify_failures(records) -> None:
    """Classify root cause of every failed PLR (check-plr-errors.sh logic)."""
    failed = [rec for rec in records if rec.succeeded_status == "False"]
    for rec in failed:
        print()
        print(f"{YELLOW}PipelineRun: {rec.name}{RESET}")
        print(f"Status: False | Reason: {rec.succeeded_reason}{RESET}")
        print(f"Message: {rec.succeeded_message}{RESET}")
        reason = rec.succeeded_reason
        if reason == "Cancelled":
            print(f"{BLUE}PipelineRun was cancelled.{RESET}")
            continue
        if reason == "PipelineRunPending":
            print(f"{BLUE}PipelineRun was left pending (never started).{RESET}")
            print("Likely kueue/scheduling issue.")
            continue
        if reason == "CouldntGetTask":
            print(f"{BLUE}PipelineRun could not resolve a Task reference.{RESET}")
            continue
        if reason == "PipelineRunTimeout":
            print(f"{BLUE}PipelineRun timed out.{RESET}")
            print("Check timing analysis to see which task was still running.")
            continue
        print(f"{RED}PipelineRun failed ({reason}). Checking TaskRuns...{RESET}")
        found_failed = False
        for tr in rec.taskruns:
            if tr.succeeded_status == "True":
                continue
            found_failed = True
            _report_failed_taskrun(tr)
        if not found_failed:
            if rec.skipped_stopping > 0:
                print(
                    f" ⤷ {BLUE}PipelineRun was stopped mid-run (all TaskRuns succeeded, "
                    f"{rec.skipped_stopping} tasks skipped with 'PipelineRun was stopping'){RESET}"
                )
            else:
                print(
                    f" ⤷ {YELLOW}No failed TaskRun found among childReferences "
                    f"despite PipelineRun reporting failure{RESET}"
                )


def run_errors(store: CacheStore) -> int:
    """Errors analysis over the cache (cache only, no cluster)."""
    plr_histogram = collect_plr_conditions(store.plrs.values())
    _print_histogram("PipelineRun conditions", plr_histogram)

    taskrun_histograms = collect_taskrun_conditions(store.taskruns.values())
    _print_grouped_histograms("TaskRun conditions (per task)", taskrun_histograms)

    phases, terminated, waiting, false_conditions = collect_pod_data(store.pods.values())
    _print_histogram("Pod phases", phases)
    _print_grouped_histograms("Container terminated reasons (per container)", terminated)
    _print_grouped_histograms("Container waiting reasons (per container)", waiting)
    _print_histogram("Pod condition failures (status=False)", false_conditions)

    classify_failures(store.plrs.values())
    return 0


def cmd_errors(args: argparse.Namespace) -> int:
    """CLI entry for 'errors'."""
    store = CacheStore(Path(args.cache)).load()
    return run_errors(store)


# ---------------------------------------------------------------------------
# 10. CLI
# ---------------------------------------------------------------------------


def add_cache_arg(parser: argparse.ArgumentParser) -> None:
    """Add the shared --cache flag."""
    parser.add_argument(
        "--cache",
        default=os.environ.get(DEFAULT_CACHE_ENV, DEFAULT_CACHE),
        help=f"cache directory (default: ${DEFAULT_CACHE_ENV} or {DEFAULT_CACHE})",
    )


def add_concurrency_arg(parser: argparse.ArgumentParser) -> None:
    """Add the shared --concurrency flag (single default)."""
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help=f"number of parallel PLRs to process (default: {DEFAULT_CONCURRENCY})",
    )


def add_timing_output_args(parser: argparse.ArgumentParser) -> None:
    """Add the --gantt-chart / --summary flags (timing + download passthrough)."""
    parser.add_argument(
        "--gantt-chart",
        metavar="PATH",
        help="render a Gantt chart to this PNG file (only when requested)",
    )
    parser.add_argument(
        "--summary",
        metavar="FILE",
        help="write aggregate timing stats as JSON to this file (only when requested)",
    )


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the full argparse interface."""
    parser = argparse.ArgumentParser(
        prog="plrtool",
        description="PipelineRun toolkit for kube-shard load tests.",
    )
    parser.add_argument("--debug", action="store_true", help="debug level to stderr is DEBUG")
    parser.add_argument("--verbose", action="store_true", help="debug level to stderr is INFO")
    parser.add_argument(
        "--log-file", default=LOG_FILENAME, help=f"log file (default: {LOG_FILENAME})"
    )
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    download = subparsers.add_parser(
        "download", help="fetch PLR manifests (+details) into the cache"
    )
    add_selector_args(download)
    add_cache_arg(download)
    add_concurrency_arg(download)
    download.add_argument(
        "--also-incomplete", action="store_true", help="cache PLR even if not completed"
    )
    details = download.add_mutually_exclusive_group()
    details.add_argument(
        "--details-if-failed",
        action="store_true",
        help="fetch TR/pod/logs for failed, finished PLRs",
    )
    details.add_argument(
        "--details-included", action="store_true", help="fetch TR/pod/logs for every PLR"
    )
    download.add_argument(
        "--with-timing",
        action="store_true",
        help="after download, run timing analysis on in-memory data",
    )
    add_timing_output_args(download)
    download.add_argument(
        "--with-errors",
        action="store_true",
        help="after download, run errors analysis on in-memory data",
    )
    download.add_argument(
        "--ka-context", default=None, help="kubeconfig context pointing at KubeArchive"
    )
    download.add_argument(
        "--ka-conf",
        default=None,
        metavar="FILE",
        help=(
            "kubectl-ka.conf mapping cluster -> KubeArchive host "
            f"(default: ${DEFAULT_KA_CONF_ENV} or {DEFAULT_KA_CONF})"
        ),
    )

    wait = subparsers.add_parser("wait", help="wait for PLR(s) to complete")
    add_selector_args(wait)
    add_cache_arg(wait)
    add_concurrency_arg(wait)
    wait.add_argument(
        "--timeout", default=DEFAULT_TIMEOUT, help=f"per-PLR timeout (default: {DEFAULT_TIMEOUT})"
    )
    wait.add_argument(
        "--dump-completed", action="store_true", help="dump completed PLR manifests into the cache"
    )

    timing = subparsers.add_parser(
        "timing", help="analyze cached succeeded PLR timings (cache only)"
    )
    add_cache_arg(timing)
    add_timing_output_args(timing)

    errors = subparsers.add_parser(
        "errors", help="histogram + classify failures in the cache (cache only)"
    )
    add_cache_arg(errors)

    return parser


COMMANDS = {
    "download": cmd_download,
    "wait": cmd_wait,
    "timing": cmd_timing,
    "errors": cmd_errors,
}


def main(argv: list[str] | None = None) -> int:
    """Entry point: parse args, set up logging, dispatch to a subcommand."""
    args = build_arg_parser().parse_args(argv)
    setup_logging(args.log_file, verbose=args.verbose, debug=args.debug)
    command = COMMANDS[args.subcommand]
    return command(args)


if __name__ == "__main__":
    sys.exit(main())
