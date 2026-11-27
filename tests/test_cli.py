"""Unit tests for CLI argparse wiring and subcommand option dataclasses."""

from pathlib import Path

from helpers import parse_cli

import plrtool
from plrtool import DownloadOptions, TimingOptions


def test_cli_subcommand_help_and_defaults():
    args = parse_cli(["download", "--namespace", "n", "--plr", "p"])
    assert args.subcommand == "download"
    assert args.concurrency == plrtool.DEFAULT_CONCURRENCY
    assert args.cache == "collected-data"
    # default log file lives under the XDG data dir (~/.local/share/plrtool).
    assert args.log_file == plrtool.DEFAULT_LOG_FILE
    assert args.log_file.endswith(".local/share/plrtool/plrtool.log")

    wait_args = parse_cli(["wait", "--namespace", "n", "--plr", "p"])
    assert wait_args.timeout == "100m"
    assert wait_args.delete_on_timeout is False

    wait_args = parse_cli(["wait", "--namespace", "n", "--plr", "p", "--delete-on-timeout"])
    assert wait_args.delete_on_timeout is True

    timing_args = parse_cli(["timing", "--gantt-chart", "g.png", "--summary", "s.json"])
    assert timing_args.gantt_chart == "g.png"
    assert timing_args.summary == "s.json"
    assert timing_args.gantt_no_taskruns is False
    assert timing_args.gantt_no_steps is False

    timing_args = parse_cli(["timing", "--gantt-no-taskruns", "--gantt-no-steps"])
    assert timing_args.gantt_no_taskruns is True
    assert timing_args.gantt_no_steps is True
    histogram_args = parse_cli(
        [
            "timing",
            "--start-histogram",
            "starts.png",
            "--duration-histogram",
            "durations.png",
            "--histogram-buckets",
            "42",
        ]
    )
    assert histogram_args.start_histogram == "starts.png"
    assert histogram_args.duration_histogram == "durations.png"
    assert histogram_args.histogram_buckets == 42
    plr_args = parse_cli(["timing", "--plr", "my-plr"])
    assert plr_args.plr == "my-plr"
    assert parse_cli(["timing"]).plr is None
    assert parse_cli(["timing", "--details-json", "d.json"]).details_json == "d.json"
    assert parse_cli(["timing"]).details_json is None
    # download passthrough: --with-timing accepts the same output flags.
    dl_args = parse_cli(
        ["download", "--namespace", "n", "--plr", "p", "--with-timing", "--details-json", "d.json"]
    )
    assert dl_args.details_json == "d.json"


def test_cli_delete_defaults_and_override():
    args = parse_cli(["delete", "--namespace", "n", "--plr", "p"])
    assert args.timeout == "1m"
    assert args.concurrency == plrtool.DEFAULT_CONCURRENCY

    args = parse_cli(
        ["delete", "--namespace", "n", "--plr", "p", "--timeout", "5s", "--concurrency", "3"]
    )
    assert args.timeout == "5s"
    assert args.concurrency == 3


def test_cli_download_with_timing_builds_options():
    args = parse_cli(
        [
            "download",
            "--namespace",
            "n",
            "--plr",
            "p",
            "--with-timing",
            "--gantt-chart",
            "g.png",
        ]
    )
    options = DownloadOptions(
        cache_dir=Path(args.cache),
        concurrency=args.concurrency,
        with_timing=(
            TimingOptions(gantt_chart=args.gantt_chart, summary=args.summary)
            if args.with_timing
            else None
        ),
    )
    assert options.with_timing.gantt_chart == "g.png"
    assert options.with_timing.summary is None
