"""Unit tests for CLI argparse wiring and subcommand option dataclasses."""

from pathlib import Path

import plrtool
from plrtool import DownloadOptions, TimingOptions

from helpers import parse_cli


def test_cli_subcommand_help_and_defaults():
    args = parse_cli(["download", "--namespace", "n", "--plr", "p"])
    assert args.subcommand == "download"
    assert args.concurrency == plrtool.DEFAULT_CONCURRENCY
    assert args.cache == "collected-data"

    wait_args = parse_cli(["wait", "--namespace", "n", "--plr", "p"])
    assert wait_args.timeout == "100m"

    timing_args = parse_cli(["timing", "--gantt-chart", "g.png", "--summary", "s.json"])
    assert timing_args.gantt_chart == "g.png"
    assert timing_args.summary == "s.json"


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
