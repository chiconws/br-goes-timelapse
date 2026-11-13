# Contributing to GOES Timelapse

Thanks for helping. The tooling mirrors Home Assistant core, so if you have contributed there, this will feel familiar. Python is 3.13, the `python3` of the add-on image.

## Setup

```bash
uv venv --python 3.13 .venv
uv pip install --python .venv -r requirements_test.txt
.venv/bin/prek install
```

`prek` is a drop-in for `pre-commit`; `pre-commit install` works too. The hook runs ruff (lint and format), codespell, yamllint, prettier, zizmor, markdownlint, mypy and Pyright on every commit.

## Checks

```bash
SKIP=no-commit-to-branch .venv/bin/prek run --all-files --hook-stage manual
.venv/bin/pytest
```

`SKIP` lets the check run on `dev`/`main`; `--hook-stage manual` adds the manual-only hooks. CI runs the same command plus the tests with branch coverage; coverage must not drop below the floor in `pyproject.toml`.

## Pull requests

- Branch from `dev` and open the PR against `dev` (`main` only receives releases).
- One topic per PR; use conventional commit titles (`feat:`, `fix:`, `chore:`, `docs:`).
- Code, comments and log messages in English; UI text and docs in Brazilian Portuguese.
- Behaviour changes bump `version` in `goes_timelapse/config.yaml` and add an entry to `goes_timelapse/CHANGELOG.md`.
- Code structure and conventions are in [AGENTS.md](AGENTS.md).
