"""timing analysis over cached *succeeded* PipelineRuns (cache only)."""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

from .cache import CacheStore
from .constants import BLUE, RESET, TD_FMT, YELLOW
from .log import logger
from .options import TimingOptions
from .records import PLRRecord, PodRecord, TaskRunRecord
from .utils import duration_seconds, epoch_of, fmt_ts, percentile

__all__ = ["cmd_timing", "run_timing"]
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
        f"  {label:<20} min={min(values)}s avg={sum(values) / len(values):.1f}s "
        f"p99={p9999}s max={max(values)}s"
    )


def _print_ts_range(label: str, values: list) -> None:
    """Print a min/max timestamp line with the span (original summary format)."""
    if not values:
        print(f"  {label:<20} min={'n/a':<22} max={'n/a':<22} span=n/a")
        return

    def fmt(value: int) -> str:
        return dt.datetime.fromtimestamp(value, tz=dt.UTC).strftime(TD_FMT)

    span = max(values) - min(values)
    print(f"  {label:<20} min={fmt(min(values)):<22} max={fmt(max(values)):<22} span={span}s")


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
    """Collects (label, start, end, color) segments and renders a matplotlib chart.

    One horizontal row per label; repeated ``add`` calls for the same label draw
    segments head-to-tail on that row (e.g. blue pending followed by red running
    for one object, red starting exactly where blue ends).
    """

    # 0 = pending (blue), 1 = running (red) - same colors as the gnuplot chart.
    COLORS = ("#0000FF", "#FF0000")

    def __init__(self) -> None:
        self.rows: dict[str, list[tuple[dt.datetime, dt.datetime, int]]] = {}

    def add(
        self,
        label: str,
        start: dt.datetime | None,
        end: dt.datetime | None,
        color: int,
    ) -> None:
        """Register one bar segment on the ``label`` row (skipped when a timestamp is missing)."""
        if start is None or end is None:
            return
        if end < start:
            start, end = end, start
        self.rows.setdefault(label, []).append((start, end, color))

    def render(self, output: str) -> None:
        """Render the Gantt chart to the given PNG path."""
        if not self.rows:
            print("Skipped generating Gantt diagram because data were missing")
            return
        import matplotlib
        import matplotlib.pyplot as plt
        from matplotlib import dates as mdates

        matplotlib.use("Agg")
        labels = list(self.rows)
        fig = plt.figure(figsize=(12, min(max(len(labels) * 0.4, 4.0), 40.0)), dpi=100)
        ax = fig.add_subplot(111)
        numbers = []
        all_x: list[float] = []
        for label in labels:
            segments = []
            for start, end, color in self.rows[label]:
                x0 = mdates.date2num(start)
                x1 = mdates.date2num(end)
                segments.append((x0, x1, color))
                all_x.extend((x0, x1))
            numbers.append((label, segments))
        x_min, x_max = min(all_x), max(all_x)
        padding = max((x_max - x_min) * 0.02, 60.0 / 86400.0)
        for y, (label, segments) in enumerate(numbers):
            for x0, x1, color in segments:
                ax.barh(y, x1 - x0, left=x0, height=0.6, color=self.COLORS[color])
        ax.set_xlim(x_min - padding, x_max + padding)
        ax.invert_yaxis()
        ax.set_yticks(range(len(numbers)))
        ax.set_yticklabels([label for label, _segs in numbers], fontsize=8)
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
    gantt = _Gantt()

    for rec in plrs:
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

        gantt.add(f"PLR {rec.name}", rec.created, rec.started, 0)
        if rec.started is not None:
            gantt.add(f"PLR {rec.name}", rec.started, rec.completed, 1)

        if options.details:
            _print_plr_header(rec)

        trs_earliest: dt.datetime | None = None
        for tr in sorted(
            rec.taskruns, key=lambda item: item.created or dt.datetime.min.replace(tzinfo=dt.UTC)
        ):
            if tr.created is not None and (trs_earliest is None or tr.created < trs_earliest):
                trs_earliest = tr.created
            if options.details:
                _print_tr(tr)
            gantt.add(f"TR {tr.pipeline_task}", tr.created, tr.started, 0)
            if tr.started is not None:
                gantt.add(f"TR {tr.pipeline_task}", tr.started, tr.completed, 1)
            if tr.steps:
                first_step: dt.datetime | None = None
                for step in tr.steps:
                    duration = duration_seconds(step.started_at, step.finished_at)
                    reason = step.reason or "n/a"
                    if options.details:
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
                    if options.details:
                        print(
                            f"     {YELLOW}TaskRun wait time (creation to first step): {tr_wait}s{RESET}"
                        )
            if options.details and tr.pod is not None:
                _print_pod(tr.pod)
            if options.details:
                print()
        if rec.created is not None and trs_earliest is not None:
            plr_wait = int((trs_earliest - rec.created).total_seconds())
            if options.details:
                print(
                    f" ⤷ {YELLOW}PipelineRun wait time (creation to first TaskRun): {plr_wait}s{RESET}"
                )
        if options.details:
            print()

    if options.gantt_chart:
        gantt.render(options.gantt_chart)

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
        print(f"         {ctype!s:<20} {cstatus or '-'!s:<6} {creason or '-'!s:<25} {ctime or '-'}")
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
    options = TimingOptions(
        gantt_chart=args.gantt_chart, summary=args.summary, details=args.details
    )
    return run_timing(store, options)
