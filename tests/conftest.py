"""Shared fixtures: settings on tmp_path with bundled test assets."""

from __future__ import annotations

from collections.abc import Generator
import gzip
import json
from pathlib import Path

from fastapi.testclient import TestClient
import pytest

from goes_timelapse.app import create_app
from goes_timelapse.core.config import Settings

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "municipalities_fixture.json"
STATE_FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "state_boundaries_fixture.json"
)
GEOMETRY_FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "area_geometries_fixture.json"
)


def write_catalog_asset(target_path: Path) -> Path:
    """Write the fixture catalog as a gzipped asset."""
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    with gzip.open(target_path, "wt", encoding="utf-8") as handle:
        json.dump(payload, handle)
    return target_path


def write_state_boundary_asset(target_path: Path) -> Path:
    """Write the fixture state boundaries as a gzipped asset."""
    payload = json.loads(STATE_FIXTURE_PATH.read_text(encoding="utf-8"))
    with gzip.open(target_path, "wt", encoding="utf-8") as handle:
        json.dump(payload, handle)
    return target_path


def write_geometry_cache(target_dir: Path) -> Path:
    """Seed the IBGE geometry cache with fixture outlines."""
    payload = json.loads(GEOMETRY_FIXTURE_PATH.read_text(encoding="utf-8"))
    target_dir.mkdir(parents=True, exist_ok=True)
    for area_id, geometry in payload.items():
        with gzip.open(
            target_dir / f"{area_id}.json.gz", "wt", encoding="utf-8"
        ) as handle:
            json.dump(geometry, handle)
    return target_dir


def build_settings(tmp_path: Path) -> Settings:
    """Build settings with every path under tmp_path."""
    catalog_path = write_catalog_asset(tmp_path / "areas.json.gz")
    state_boundaries_path = write_state_boundary_asset(
        tmp_path / "state_boundaries.json.gz"
    )
    geometry_cache_dir = write_geometry_cache(tmp_path / "data" / "geometry")
    settings = Settings(
        host="127.0.0.1",
        port=8099,
        goes_url="https://example.invalid/",
        poll_minutes=1,
        frame_count=3,
        gif_fps=2,
        solar_margin_hours=2,
        max_tracked=5,
        log_level=20,
        data_dir=tmp_path / "data",
        state_dir=tmp_path / "state",
        raw_dir=tmp_path / "data" / "raw",
        processed_dir=tmp_path / "data" / "processed",
        geometry_cache_dir=geometry_cache_dir,
        media_dir=tmp_path / "media",
        snippets_dir=tmp_path / "config" / "goes_timelapse" / "lovelace",
        db_path=tmp_path / "state" / "state.db",
        catalog_path=catalog_path,
        state_boundaries_path=state_boundaries_path,
        ibge_malhas_url="https://example.invalid/api/v4/malhas",
        ibge_request_timeout=5,
        font_path=Path("C:/Windows/Fonts/arial.ttf"),
        allowed_client_hosts=("testclient", "127.0.0.1", "::1"),
        max_render_dimension=900,
    )
    settings.ensure_directories()
    return settings


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    """Settings on tmp_path with test assets."""
    return build_settings(tmp_path)


@pytest.fixture
def client(settings: Settings) -> Generator[TestClient, None, None]:
    """API test client without background tasks."""
    app = create_app(settings=settings, start_background_tasks=False)
    with TestClient(app) as test_client:
        yield test_client
