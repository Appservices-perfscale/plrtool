"""download + wait subcommands: collection, retry, and the shared cache path.

``wait --dump-completed`` reuses the download pipeline via
``collect_plr_manifest``/``_dump_completed_plrs``.
"""

from __future__ import annotations

import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from .cache import CacheStore
from .cluster import Cluster
from .errors import run_errors
from .exceptions import ClusterError
from .graph import link_plr_taskruns
from .log import logger
from .options import DownloadOptions, TimingOptions, WaitOptions
from .records import PLRRecord, parse_plr
from .targets import Target, resolve_targets
from .timing import run_timing
from .utils import parse_duration

__all__ = [
    "cmd_download",
    "cmd_wait",
    "collect_one",
    "collect_plr_manifest",
    "run_download",
    "run_wait",
]
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
    (same cached-wins policy as collect_plr_manifest).  The PLR -> TaskRun ->
    Pod graph is assembled through the shared ``link_plr_taskruns`` seam - the
    same one the offline ``CacheStore.load()`` uses - so the in-memory picture
    of a cache cannot drift from a later offline load of the same directory.
    """
    for tr_name in plr.tr_refs:
        raw_tr = store.read_cached_object("taskrun", tr_name)
        if raw_tr is None:
            raw_tr = cluster.get_object("taskrun", target.namespace, tr_name)
        if raw_tr is None:
            logger.warning("TaskRun %s/%s not found; skipping", target.namespace, tr_name)
            continue
        tr = store.add_taskrun(raw_tr, target.namespace)
        pod_name = tr.pod_name
        if not pod_name:
            continue
        raw_pod = store.read_cached_object("pod", pod_name)
        if raw_pod is None:
            raw_pod = cluster.get_object("pod", target.namespace, pod_name)
        if raw_pod is None:
            logger.warning("Pod %s/%s not found; skipping", target.namespace, pod_name)
        else:
            store.add_pod(raw_pod, target.namespace)
        for container in tr.log_containers:
            text = store.read_cached_log(pod_name, container)
            if text is None:
                text = cluster.get_logs(target.namespace, pod_name, container)
            if text is not None:
                store.add_log(pod_name, container, text)
    link_plr_taskruns(plr, store.taskruns, store.pods)


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
            TimingOptions(gantt_chart=args.gantt_chart, summary=args.summary, details=args.details)
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
