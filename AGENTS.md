# Agent Notes: GOES Timelapse

Language: README, DOCS, CHANGELOG and UI in **pt-BR**; code, comments and log messages in English.

## Overview

Home Assistant app (Supervisor add-on, `amd64`) that builds WebP timelapses of GOES-19 imagery for Brazilian municipalities. It reads only the needed rows of NOAA's `ABI-L1b-RadF` C02/C13 and the `GLM-L2-LCFA` flashes over HTTP Range, renders frames locally and publishes them to `/media/goes_timelapse/`. The UI is served through ingress (FastAPI on port 8099).

## Code structure

Add-on folder: `goes_timelapse/` (`config.yaml`, `build.yaml`, `Dockerfile`, `rootfs/`, `DOCS.md`, `CHANGELOG.md`). Python package: `goes_timelapse/src/goes_timelapse/`.

- `app.py`: FastAPI app, HTTP API and static UI; `__main__.py` starts uvicorn.
- `core/`: settings from add-on options (`config.py`), SQLite state store (`state.py`), models, logging.
- `data/`: municipality catalog and IBGE geometry (`assets/*.json.gz`).
- `pipeline/`: NOAA listing and partial downloads (`downloader.py`, `partial_read.py`), raw cache, rendering, storage guard, the refresh loop (`service.py`).
- `timeline/`: frame plan and day/night logic (`solar.py`).
- `static/`: frontend (plain JS/CSS, no build step).
- `scripts/build_area_catalog.py`: rebuilds `assets/areas.json.gz` from IBGE data.
- `tests/`: pytest suite; `tests/helpers.py` writes synthetic netCDF/GeoTIFF fixtures.

## Key concepts

- Bump `version` in `goes_timelapse/config.yaml` and add a `CHANGELOG.md` entry for every release; Supervisor offers the update from that version.
- Images are built on the device from `Dockerfile` (no prebuilt `image:`), so runtime deps live in `goes_timelapse/requirements.txt`.
- One cache folder per municipality; GLM flashes are shared.

## Local validation (before push)

```bash
SKIP=no-commit-to-branch .venv/bin/prek run --all-files --hook-stage manual   # ruff, format, codespell, yamllint, prettier, zizmor, markdownlint, mypy, Pyright
.venv/bin/coverage run -m pytest -q && .venv/bin/coverage report
```

Git hook: `.venv/bin/prek install` once per clone. Contributor setup: `CONTRIBUTING.md`.

## Deviations from standard

- Not a Home Assistant integration: no `custom_components/`, manifest, `hacs.json`, config flow, translations, quality scale, HACS/hassfest workflows or Marketplace badges.
- Python **3.11** for `.venv`, CI, ruff `target-version`, mypy and Pyright: it is the `python3` of the add-on base image (Debian bookworm), and `numpy<2` has no 3.14 wheels.
- No PHACC: the tests use plain `pytest` + `pytest-asyncio`.
- `CHANGELOG.md` is kept by hand: the Supervisor shows it in the add-on update dialog.
- User-facing status strings are generated in pt-BR in Python (no translation layer); codespell ignores `erro`, `atual`, `limite`.
- Prettier runs on JSON/YAML only (the frontend is not reformatted).
- Release tag must match `version` in `goes_timelapse/config.yaml`.
