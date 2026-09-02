<!--
SPDX-FileCopyrightText: Copyright (C) 2026 jhutar
SPDX-License-Identifier: Apache-2.0
-->
# plrtool

PipelineRun toolkit for kube-shard load tests. Replaces a set of shell/Python
helpers (`collect-plrs.sh`, `check-timings.sh`, `check-errors.sh`,
`wait-to-finish.sh`, ...) with one CLI.

## Install

Requires Python >= 3.12 and [uv](https://docs.astral.sh/uv/).

```bash
uv sync                 # create venv + install deps (from uv.lock)
make bootstrap          # dev tools + pre-commit hooks (first time)
```

## Usage

```bash
uv run plrtool download --namespace NS --plr NAME --cache DIR
uv run plrtool download --csv targets.csv --cache DIR --details-if-failed
uv run plrtool wait     --csv targets.csv --timeout 100m --dump-completed
uv run plrtool timing   --cache DIR --gantt-chart chart.png --summary stats.json
uv run plrtool errors   --cache DIR
```

### Subcommands

| command    | does                                                              |
|------------|-------------------------------------------------------------------|
| `download` | fetch PLR manifests (+ TaskRun/Pod/container logs) into a cache   |
| `wait`     | poll PLRs until `status.completionTime` is set (canary first)     |
| `timing`   | aggregate timing stats of cached *succeeded* PLRs (cache only)    |
| `errors`   | histogram conditions/reasons + classify failures (cache only)     |

## Cache layout

`--cache DIR` (default `$PLR_CACHE_DIR` or `./collected-data`) holds one JSON
file per fetched object plus one log file per container:

```
collected-pipelinerun-NAME.json
collected-taskrun-NAME.json
collected-pod-NAME.json
pod-POD-CONTAINER.log
```

`managedFields` are stripped on dump; legacy `{items:[...]}` YAML cache files
are still read (and re-dumped as JSON). `timing`/`errors` are offline: they
only read what is already in the cache.

## Development

```bash
make test        # full pytest suite
make check-all   # all pre-commit checks (ruff, format, mypy, bandit, gitleaks, ...)
make typecheck   # static type checking (mypy)
make audit       # dependency vulnerability check (uv/OSV)
```

## Project layout

```
src/plrtool/          Python package (src layout)
  cli.py              argparse wiring + entry point
  download.py         download + wait subcommands
  timing.py           timing analysis
  errors.py           failure analysis
  cluster.py          Kubernetes / KubeArchive access
  cache.py            on-disk JSON cache + in-memory records
  records.py          manifest -> record dataclasses
  targets.py          target selectors (--namespace/--plr, --csv)
  options.py          subcommand option dataclasses
  utils.py            pure helpers (time, stats, normalization)
  constants.py        shared defaults + ANSI colors
  log.py / exceptions.py
tests/                pytest suite (unit, no cluster needed)
```
