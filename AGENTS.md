@README.md

## Development workflow

Always validate changes with:

```bash
make test        # run full pytest suite
make check-all   # run all pre-commit checks (ruff, format, bandit, gitleaks, etc.)
```

Both must pass before committing. This is the preferred way to iterate during development.

## Project structure

- `src/prometheus_cli/` — source code (Python package)
- `tests/unit/` — unit tests
- `tests/integration/` — integration tests
- `specs/` — feature specifications and design docs
