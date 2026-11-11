"""Tests for the HTTP API."""

from __future__ import annotations

from fastapi.testclient import TestClient

from goes_timelapse.app import STATIC_ASSET_VERSION, create_app
from goes_timelapse.core.models import AreaCatalogEntry
from goes_timelapse.core.state import StateStore
from goes_timelapse.pipeline.service import GoesTimelapseService
from goes_timelapse.timeline import (
    PHASE_VISIBLE,
    SOURCE_LIGHTNING,
    SOURCE_VISIBLE,
    AreaTimelinePlan,
    TimelineFrame,
)
from tests.helpers import create_test_tif


def test_root_page_serves_js_shell(client):
    """Test root page serves js shell."""
    response = client.get("/")

    assert response.status_code == 200
    assert "GOES timelapses de municípios" in response.text
    assert f"static/app-{STATIC_ASSET_VERSION}.js" in response.text
    assert f"static/app-core.js?v={STATIC_ASSET_VERSION}" in response.text
    assert f"static/app-render.js?v={STATIC_ASSET_VERSION}" in response.text
    assert f"static/app-actions.js?v={STATIC_ASSET_VERSION}" in response.text
    assert 'id="area-search"' in response.text


def test_root_page_uses_ingress_header_for_base_href(client):
    """Test root page uses ingress header for base href."""
    response = client.get(
        "/", headers={"X-Ingress-Path": "/api/hassio_ingress/example-token"}
    )

    assert response.status_code == 200
    assert 'data-base-path="/api/hassio_ingress/example-token/"' in response.text


def test_static_helper_script_is_served(client):
    """Test static helper script is served."""
    response = client.get("/static/app-core.js")

    assert response.status_code == 200
    assert "window.GoesTimelapseApp" in response.text


def test_api_search_pin_process_and_remove(client, settings):
    """Test api search pin process and remove."""
    search_response = client.get("/api/areas", params={"q": "sumare"})
    assert search_response.status_code == 200
    assert search_response.json()[0]["area_id"] == "municipio-3552403"

    service: GoesTimelapseService = client.app.state.service

    async def fake_refresh():
        return None

    service.refresh_raw_frames = fake_refresh

    add_response = client.put("/api/tracked/municipio-3552403")
    assert add_response.status_code == 200

    visible_dir = settings.raw_dir / "visible" / "municipio-3552403"
    visible_dir.mkdir(parents=True)
    create_test_tif(visible_dir / "20260011220_frame.tif", offset=0)
    create_test_tif(visible_dir / "20260011230_frame.tif", offset=5)
    create_test_tif(visible_dir / "20260011240_frame.tif", offset=10)
    service._build_area_timeline_plan = lambda area, reference_moment=None: (
        AreaTimelinePlan(
            area_id=area.area_id,
            frames=(
                TimelineFrame(
                    slot_timestamp="20260011220",
                    phase=PHASE_VISIBLE,
                    primary_source=SOURCE_VISIBLE,
                    blend_source=None,
                    blend_alpha=None,
                    required_sources=(SOURCE_VISIBLE, SOURCE_LIGHTNING),
                ),
                TimelineFrame(
                    slot_timestamp="20260011230",
                    phase=PHASE_VISIBLE,
                    primary_source=SOURCE_VISIBLE,
                    blend_source=None,
                    blend_alpha=None,
                    required_sources=(SOURCE_VISIBLE, SOURCE_LIGHTNING),
                ),
                TimelineFrame(
                    slot_timestamp="20260011240",
                    phase=PHASE_VISIBLE,
                    primary_source=SOURCE_VISIBLE,
                    blend_source=None,
                    blend_alpha=None,
                    required_sources=(SOURCE_VISIBLE, SOURCE_LIGHTNING),
                ),
            ),
        )
    )
    rendered = service._process_area("municipio-3552403")
    assert rendered is not None

    tracked_response = client.get("/api/tracked")
    payload = tracked_response.json()
    assert payload[0]["status"] == "ready"
    assert payload[0]["media_exists"] is True

    delete_response = client.delete("/api/tracked/municipio-3552403")
    assert delete_response.status_code == 200


def test_api_sets_and_clears_marker_for_tracked_area(client):
    """Test api sets and clears marker for tracked area."""
    service: GoesTimelapseService = client.app.state.service

    async def fake_refresh():
        return None

    service.refresh_raw_frames = fake_refresh

    add_response = client.put("/api/tracked/municipio-3205309")
    assert add_response.status_code == 200

    area = service.catalog.get("municipio-3205309")
    assert area is not None
    geometry = service.geometry_store.load_geometry(area)

    save_response = client.put(
        "/api/tracked/municipio-3205309/marker",
        json={
            "lat": geometry.centroid[1],
            "lon": geometry.centroid[0],
        },
    )
    assert save_response.status_code == 200
    assert save_response.json()["marker_lat"] == geometry.centroid[1]
    assert save_response.json()["marker_lon"] == geometry.centroid[0]

    tracked_response = client.get("/api/tracked")
    payload = tracked_response.json()
    assert payload[0]["marker_lat"] == geometry.centroid[1]
    assert payload[0]["marker_lon"] == geometry.centroid[0]

    remove_response = client.delete("/api/tracked/municipio-3205309/marker")
    assert remove_response.status_code == 200
    assert remove_response.json()["marker_lat"] is None
    assert remove_response.json()["marker_lon"] is None


def test_api_rejects_marker_outside_municipality(client):
    """Test api rejects marker outside municipality."""
    service: GoesTimelapseService = client.app.state.service

    async def fake_refresh():
        return None

    service.refresh_raw_frames = fake_refresh

    add_response = client.put("/api/tracked/municipio-3205309")
    assert add_response.status_code == 200

    response = client.put(
        "/api/tracked/municipio-3205309/marker",
        json={"lat": 0.0, "lon": 0.0},
    )

    assert response.status_code == 422
    assert (
        response.json()["detail"]
        == "As coordenadas precisam estar dentro da área da imagem"
    )


def test_startup_removes_no_longer_supported_tracked_areas(settings):
    """Test startup removes no longer supported tracked areas."""
    stale_area = AreaCatalogEntry(
        area_id="regiao-imediata-320001",
        area_type="regiao-imediata",
        area_code="320001",
        name="Vitoria",
        search_text="regiao imediata 320001 vitoria es espirito santo",
        population=None,
        state_code="ES",
        state_name="Espirito Santo",
        parent_code="3201",
        parent_name="Vitoria",
    )
    seeded_store = StateStore(settings.db_path)
    seeded_store.upsert_tracked(stale_area, status="queued")
    seeded_store.close()

    app = create_app(settings=settings, start_background_tasks=False)
    with TestClient(app):
        service: GoesTimelapseService = app.state.service
        assert service.tracked() == []
