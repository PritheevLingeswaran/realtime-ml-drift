# CI Troubleshooting

Use the same interpreter and commands as GitHub Actions:

```bash
python3.11 -m venv .venv-ci
. .venv-ci/bin/activate
python -m pip install --upgrade pip wheel setuptools
python -m pip install --prefer-binary -r requirements.txt
python -m pip check
python -m ruff check src tests scripts evaluation
python -m pytest -ra --durations=20
```

Common failure modes:

- Wrong Python version: this repo is pinned for Python `3.11`. Python `3.13` may try to build older scientific packages from source.
- Dependency drift: re-run `python -m pip check` after installs to catch incompatible or missing packages.
- Runner drift: GitHub Actions now uses `ubuntu-24.04` and Python `3.11.11` to avoid surprises from `ubuntu-latest` changes.
- Network/package hiccups: CI retries dependency installation up to 3 times and prefers wheels to reduce scientific package build failures.
- CI cancellations: superseded runs are cancelled for non-`main` refs so stale branch jobs do not pile up.
- Lint mismatch: use `python -m ruff check src tests scripts evaluation` locally, not a different target set.
- Slow or flaky tests: default CI runs only `tests/` via pytest config. The workflow prints the slowest 20 tests to make regressions visible before they become timeout failures.

If GitHub Actions still fails, compare the failing log with:

```bash
python --version
python -m pip freeze
```
