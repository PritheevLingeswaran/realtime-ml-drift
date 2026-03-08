# CI Troubleshooting

Use the same interpreter and commands as GitHub Actions:

```bash
python3.11 -m venv .venv-ci
. .venv-ci/bin/activate
python -m pip install --upgrade pip wheel setuptools
python -m pip install -r requirements.txt
python -m pip check
python -m ruff check src tests scripts evaluation
python -m pytest
```

Common failure modes:

- Wrong Python version: this repo is pinned for Python `3.11`. Python `3.13` may try to build older scientific packages from source.
- Dependency drift: re-run `python -m pip check` after installs to catch incompatible or missing packages.
- CI-only cancellations: the workflow concurrency group is scoped per ref/PR. Push runs are not auto-cancelled; only superseded PR runs are.
- Lint mismatch: use `python -m ruff check src tests scripts evaluation` locally, not a different target set.
- Slow or flaky tests: default CI runs only `tests/` via pytest config. Benchmark and soak scripts under `scripts/` are imported by tests but not executed as standalone CI jobs.

If GitHub Actions still fails, compare the failing log with:

```bash
python --version
python -m pip freeze
```
