@README.md

## Development workflow

Always validate changes with:

```bash
make test        # run full pytest suite
make check-all   # run all pre-commit checks (ruff, format, bandit, gitleaks, etc.)
```

Both must pass before committing. This is the preferred way to iterate during development.

## Project structure

- `src/plrtool/` — source code (Python package)
- `tests/` — tests (pytest)

## Pattern References

Common change types and where to follow the existing pattern:

- New subcommand: add parser in `cli.py:84` (near the `download`/`timing` parsers) and a `cmd_*` entry; wire into `cli.py:main`. See `download.py`/`timing.py` for the `cmd_*` shape (`args` in, exit code out).
- New CLI option: add field to the matching subcommand option dataclass in `options.py`, then bind it to the argparse parser in `cli.py` (e.g. `--cache`, `--csv`).
- New cache-record field: extend the corresponding dataclass in `records.py` (e.g. `PLRRecord`) and its (de)serialization in `cache.py`; add a test in `tests/test_plrtool.py`.
- New error class: derive from `PlrtoolError` in `exceptions.py`; raise/report it from `errors.py` classifiers (`classify_failures`) or command logic.
- New Kubernetes/KubeArchive resource fetch: add the client call in `cluster.py` (lazy-import `kubernetes` inside the method, as done for PipelineRuns/TaskRuns/Pods) and persist results via `cache.py`.

Pick the closest existing example and mirror it; keep `utils.py` free of domain logic so it stays importable from anywhere.
