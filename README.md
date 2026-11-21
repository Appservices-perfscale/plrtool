# plrtool

PipelineRun toolkit is a CLI tool for downloading Tekton PipelineRuns
(& it's TaskRuns and it's pods and it's container logs), waiting for it to
finish and exploring their duration and errors.

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
uv run plrtool download --namespace NS --plr-since 2026-08-01 --plr-until 2026-08-02 --cache DIR
uv run plrtool wait     --csv targets.csv --timeout 100m --dump-completed
uv run plrtool delete   --csv targets.csv --timeout 1m
uv run plrtool timing   --cache DIR --gantt-chart chart.png --summary stats.json
uv run plrtool errors   --cache DIR
uv run plrtool normalize plr.yaml [--params-only]
```

Diff two PLRs after stripping run-specific noise:

```bash
diff -u <(plrtool normalize aaa.yaml) <(plrtool normalize bbb.yaml)
```

`normalize` drops `status` and volatile metadata (UID, resource version,
creation timestamp, managed fields, owner references, kubectl last-applied
annotation), sorts map keys and lists of named objects. `--params-only` prints
just `spec.params`. Input can be YAML or JSON.

### Logging levels

Default stderr shows WARNING and above (data gaps and failures). `--verbose`
adds INFO progress (one `[done/total]` line per PLR, phase summaries);
`--debug` adds per-API-call chatter (every fetch, cache hit, list page).
The log file (`~/.local/share/plrtool/plrtool.log`) always records DEBUG.
See the convention in `src/plrtool/log.py`.

### Time-range download

`--plr-since`/`--plr-until` (with `--namespace`) select every PipelineRun
created strictly inside the given time range, in ISO format (date-only
allowed, then midnight UTC):

```bash
uv run plrtool download --namespace NS --plr-since 2026-08-01T00:00:00Z \
    --plr-until 2026-08-02T00:00:00Z --cache DIR --details-if-failed
```

Both bounds are optional (open-ended). PLRs are discovered by listing from
the live cluster and KubeArchive (KubeArchive filters server-side, results
are unioned and deduplicated), so both still-running and already-archived
PLRs are found. Combinable with `--plr` and `--csv` (union).

### Subcommands

| command    | does                                                              |
|------------|-------------------------------------------------------------------|
| `download` | fetch PLR manifests (+ TaskRun/Pod/container logs) into a cache   |
| `wait`     | poll PLRs until `status.completionTime` is set (canary first)     |
| `delete`   | delete PLRs and wait for them to be gone (timeout per PLR)       |
| `timing`   | aggregate timing stats of cached *succeeded* PLRs (cache only)    |
| `errors`   | histogram conditions/reasons + classify failures (cache only)     |
| `normalize`| print PLR manifest with run-specific noise removed (for diffing)  |

## Cache layout

`--cache DIR` (default `$PLR_CACHE_DIR` or `./collected-data`) holds one JSON
file per fetched object plus one log file per container:

```
collected-pipelinerun-NAME.json
collected-taskrun-NAME.json
collected-pod-NAME.json
pod-POD-CONTAINER.log
```

`managedFields` are stripped on dump.
YAML cache files with `{items:[...]}` are still read (and re-dumped as JSON).
`timing`/`errors` are offline: they only read what is already in the cache.

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
  normalize.py        manifest normalization for diffing
  cluster.py          Kubernetes / KubeArchive access
  cache.py            on-disk JSON cache + in-memory records
  records.py          manifest -> record dataclasses
  graph.py            Run Graph assembly seam (PLR -> TaskRun -> Pod)
  targets.py          target selectors (--namespace/--plr, --csv)
  options.py          subcommand option dataclasses
  utils.py            pure helpers (time, stats, normalization)
  constants.py        shared defaults + ANSI colors
  log.py / exceptions.py
tests/                pytest suite (unit, no cluster needed)
```
