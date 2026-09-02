"""errors analysis: condition histograms + failure classification (cache only)."""

from __future__ import annotations

import argparse
import re
from collections import Counter, defaultdict
from collections.abc import Iterable
from pathlib import Path

from .cache import CacheStore
from .constants import BLUE, RED, RESET, YELLOW
from .records import PLRRecord, PodRecord, TaskRunRecord
from .utils import MISSING, normalize_message

__all__ = [
    "classify_failures",
    "cmd_errors",
    "collect_plr_conditions",
    "collect_pod_data",
    "collect_taskrun_conditions",
    "run_errors",
]
# ---------------------------------------------------------------------------
# 9. errors analysis
# ---------------------------------------------------------------------------


def collect_plr_conditions(plrs: Iterable[PLRRecord]) -> Counter[tuple[str, str, str]]:
    """Count (status, reason, normalized message) across PipelineRuns."""
    histogram: Counter[tuple[str, str, str]] = Counter()
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


def collect_taskrun_conditions(
    taskruns: Iterable[TaskRunRecord],
) -> defaultdict[str, Counter[tuple[str, str, str]]]:
    """Count (status, reason, message) per pipelineTask name across TaskRuns."""
    histograms: defaultdict[str, Counter[tuple[str, str, str]]] = defaultdict(Counter)
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


def collect_pod_data(
    pods: Iterable[PodRecord],
) -> tuple[
    Counter[str],
    defaultdict[str, Counter[str]],
    defaultdict[str, Counter[str]],
    Counter[tuple[str, str]],
]:
    """Return (phases, terminated-reasons, waiting-reasons, false-conditions)."""
    phases: Counter[str] = Counter()
    terminated: defaultdict[str, Counter[str]] = defaultdict(Counter)
    waiting: defaultdict[str, Counter[str]] = defaultdict(Counter)
    false_conditions: Counter[tuple[str, str]] = Counter()
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


def classify_failures(records: Iterable[PLRRecord]) -> None:
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
