"""plrtool - PipelineRun toolkit for kube-shard load tests.

Package (src layout) split from the original single-file script along its
existing internal section boundaries; the public surface below keeps the
``import plrtool; plrtool.<name>`` API identical.

Subcommands (see ``plrtool.cli`` / ``plrtool download --help``):

  download --cache DIR (--namespace NS --plr NAME | --csv FILE)
           [--concurrency N] [--also-incomplete]
           [--details-if-failed | --details-included]
           [--with-timing [--gantt-chart PATH] [--summary FILE]]
           [--with-errors] [--ka-context NAME] [--ka-conf FILE]
  wait     --cache DIR (--namespace NS --plr NAME | --csv FILE)
           [--concurrency N] [--timeout 100m] [--dump-completed]
  timing   --cache DIR [--gantt-chart PATH] [--summary FILE]
  errors   --cache DIR

Concurrency, cache path and log verbosity all have sensible defaults.
"""

__version__ = "0.1.0"

from .cache import CacheStore, strip_managed_fields
from .cli import build_arg_parser, main
from .cluster import (
    DEFAULT_KA_CONF,
    DEFAULT_KA_CONF_ENV,
    KIND_API,
    Cluster,
    ka_host_for_cluster,
    load_ka_conf,
)
from .constants import (
    BLUE,
    DEFAULT_CACHE,
    DEFAULT_CACHE_ENV,
    DEFAULT_CONCURRENCY,
    DEFAULT_TIMEOUT,
    LOG_FILENAME,
    MISSING,
    POLL_INTERVAL,
    RED,
    RESET,
    RETRIES,
    RETRY_SLEEP,
    TD_FMT,
    YELLOW,
)
from .download import (
    cmd_download,
    cmd_wait,
    collect_one,
    collect_plr_manifest,
    run_download,
    run_wait,
)
from .errors import (
    classify_failures,
    cmd_errors,
    collect_plr_conditions,
    collect_pod_data,
    collect_taskrun_conditions,
    run_errors,
)
from .exceptions import ClusterError, PlrtoolError
from .graph import link_plr_taskruns, link_run_graph
from .options import DownloadOptions, TimingOptions, WaitOptions
from .records import (
    ContainerStatusRecord,
    PLRRecord,
    PodRecord,
    StepRecord,
    TaskRunRecord,
    parse_plr,
    parse_pod,
    parse_taskrun,
    succeeded_condition,
)
from .targets import (
    CsvSelector,
    NamespacePlrSelector,
    Target,
    TargetSelector,
    add_selector_args,
    get_selectors,
    resolve_targets,
)
from .timing import cmd_timing, run_timing
from .utils import (
    duration_seconds,
    epoch_of,
    fmt_ts,
    normalize_message,
    parse_duration,
    parse_ts_dt,
    percentile,
)

__all__ = [
    "BLUE",
    "DEFAULT_CACHE",
    "DEFAULT_CACHE_ENV",
    "DEFAULT_CONCURRENCY",
    "DEFAULT_KA_CONF",
    "DEFAULT_KA_CONF_ENV",
    "DEFAULT_TIMEOUT",
    # cluster
    "KIND_API",
    # constants
    "LOG_FILENAME",
    "MISSING",
    "POLL_INTERVAL",
    "RED",
    "RESET",
    "RETRIES",
    "RETRY_SLEEP",
    "TD_FMT",
    "YELLOW",
    # cache
    "CacheStore",
    "Cluster",
    "ClusterError",
    "ContainerStatusRecord",
    "CsvSelector",
    # options
    "DownloadOptions",
    "NamespacePlrSelector",
    "PLRRecord",
    # exceptions
    "PlrtoolError",
    "PodRecord",
    # records
    "StepRecord",
    # targets
    "Target",
    "TargetSelector",
    "TaskRunRecord",
    "TimingOptions",
    "WaitOptions",
    "__version__",
    "add_selector_args",
    # cli
    "build_arg_parser",
    "classify_failures",
    "cmd_download",
    "cmd_errors",
    "cmd_timing",
    "cmd_wait",
    "collect_one",
    # errors
    "collect_plr_conditions",
    # download/wait
    "collect_plr_manifest",
    "collect_pod_data",
    "collect_taskrun_conditions",
    "duration_seconds",
    "epoch_of",
    "fmt_ts",
    "get_selectors",
    "ka_host_for_cluster",
    "link_plr_taskruns",
    "link_run_graph",
    "load_ka_conf",
    "main",
    "normalize_message",
    "parse_duration",
    "parse_plr",
    "parse_pod",
    "parse_taskrun",
    # utils
    "parse_ts_dt",
    "percentile",
    "resolve_targets",
    "run_download",
    "run_errors",
    # timing
    "run_timing",
    "run_wait",
    "strip_managed_fields",
    "succeeded_condition",
]
