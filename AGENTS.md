@README.md

## Development workflow

Always validate changes with:

```bash
make test        # run full pytest suite
make check-all   # run all pre-commit checks (ruff, format, mypy, bandit, gitleaks, ...)
make typecheck   # static type checking (mypy)
```

Both must pass before committing. This is the preferred way to iterate during development.

### Single-file verification

Fast checks for one file (no full build):

```bash
uv run ruff check path/to/file.py
uv run mypy path/to/file.py
```

Formatting check for a file: `uv run ruff format --check path/to/file.py`. Full gate
for everything: `make check-all` (ruff + mypy + bandit + gitleaks via pre-commit).

## Project structure

- `src/plrtool/` — source code (Python package)
- `tests/` — tests (pytest)

## Pattern References

Common change types and where to follow the pattern (pick the closest real example and mirror it):

- New subcommand: follow the pattern in `cli.py` (`download`/`timing`/`errors` parsers + `main`) and `download.py` for the `cmd_*` shape (args in, exit code out).
- New CLI option: follow the pattern in `options.py` and `cli.py` — use `--cache`/`--csv` as a template.
- New cache-record field: follow the pattern in `records.py` (`PLRRecord` and friends) and its (de)serialization in `cache.py`; see `cache.py` for the JSON layout.
- New error class: follow the pattern in `exceptions.py` (`PlrtoolError`) and `errors.py` classifiers.
- New Kubernetes/KubeArchive resource fetch: follow the pattern in `cluster.py` (lazy-import `kubernetes`) and persist results via `cache.py`.

Keep `utils.py` free of domain logic so it stays importable from anywhere.

### Logging convention

Authoritative spec: `src/plrtool/log.py` module docstring. Short version:

* ERROR = target failed (exit code), WARNING = data gap user must know about
* INFO = progress milestones (`--verbose`): phase starts/ends, one
  `[done/total]` line per completed PLR, summaries
* DEBUG = per-API-call chatter (`--debug`, always in the log file): every
  object fetched/cached, every list page, 404 fallbacks
* Every network operation logs exactly once: success at DEBUG (milestone at
  INFO), failure at WARNING/ERROR. No silent success paths.
