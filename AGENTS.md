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
