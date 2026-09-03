"""argparse wiring + process entry point (``main``)."""

from __future__ import annotations

import argparse
import os
import sys

from .cluster import DEFAULT_KA_CONF, DEFAULT_KA_CONF_ENV
from .constants import (
    DEFAULT_CACHE,
    DEFAULT_CACHE_ENV,
    DEFAULT_CONCURRENCY,
    DEFAULT_LOG_FILE,
    DEFAULT_TIMEOUT,
)
from .download import cmd_download, cmd_wait
from .errors import cmd_errors
from .log import setup_logging
from .targets import add_selector_args
from .timing import cmd_timing

__all__ = [
    "add_cache_arg",
    "add_concurrency_arg",
    "add_timing_output_args",
    "build_arg_parser",
    "main",
]
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
    parser.add_argument(
        "--details", action="store_true", help="print per-PipelineRun timing detail lines"
    )


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the full argparse interface."""
    parser = argparse.ArgumentParser(
        prog="plrtool",
        description="PipelineRun toolkit for kube-shard load tests.",
    )
    parser.add_argument("-d", "--debug", action="store_true", help="debug level to stderr is DEBUG")
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="debug level to stderr is INFO"
    )
    parser.add_argument(
        "--log-file", default=DEFAULT_LOG_FILE, help=f"log file (default: {DEFAULT_LOG_FILE})"
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
        "--no-logs", action="store_true", help="fetch TR/pod manifests but skip container logs"
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
