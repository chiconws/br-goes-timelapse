"""Tests for frame rendering and WebP output."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import cast

from PIL import Image
import pytest
from rasterio import Affine
from rasterio.windows import Window

from goes_timelapse.core.models import AreaGeometry
from goes_timelapse.data.catalog import AreaCatalog
from goes_timelapse.data.ibge import IbgeGeometryStore
from goes_timelapse.pipeline.rendering import (
    AreaRenderer,
    FrameSpec,
    RenderPlan,
    WebpBuilder,
    format_capture_time,
)
from tests.helpers import create_test_tif


def _window(col_off: int, row_off: int, width: int, height: int) -> Window:
    return Window.from_slices((row_off, row_off + height), (col_off, col_off + width))


def test_format_capture_time_does_not_include_brt():
    """Test format capture time does not include brt."""
    label = format_capture_time("20260702020_GOES19-ABI-FD-02-21696x21696.tif")

    assert "BRT" not in label
    assert label


def test_renderer_processes_visible_frames_and_builds_webp(settings):
    """Test renderer processes visible frames and builds webp."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    area = catalog.get("municipio-3205309")
    assert area is not None
    geometry = IbgeGeometryStore(
        settings.geometry_cache_dir,
        base_url=settings.ibge_malhas_url,
        timeout_seconds=settings.ibge_request_timeout,
    ).load_geometry(area)

    tif_paths = [
        create_test_tif(settings.raw_dir / "20260011220_frame.tif", offset=0),
        create_test_tif(settings.raw_dir / "20260011230_frame.tif", offset=5),
        create_test_tif(settings.raw_dir / "20260011240_frame.tif", offset=10),
    ]

    renderer = AreaRenderer(settings)
    png_paths = renderer.process_frames(area, geometry, tif_paths)

    assert len(png_paths) == 3
    assert all(path.exists() for path in png_paths)

    media_path = WebpBuilder(settings).build(area.area_id, png_paths)

    assert media_path.exists()


def test_large_municipalities_get_less_extra_margin_than_small_ones(settings):
    """Test large municipalities get less extra margin than small ones."""
    renderer = AreaRenderer(settings)
    small_geometry = AreaGeometry(
        area_id="small",
        centroid=(-40.3, -20.3),
        bounds=(-40.35, -20.35, -40.25, -20.25),
        polygon=(
            (-40.35, -20.35),
            (-40.25, -20.35),
            (-40.25, -20.25),
            (-40.35, -20.25),
            (-40.35, -20.35),
        ),
    )
    large_geometry = AreaGeometry(
        area_id="large",
        centroid=(-55.0, -3.0),
        bounds=(-57.0, -5.0, -53.0, -1.0),
        polygon=(
            (-57.0, -5.0),
            (-53.0, -5.0),
            (-53.0, -1.0),
            (-57.0, -1.0),
            (-57.0, -5.0),
        ),
    )

    small_bounds = renderer.destination_bounds(small_geometry)
    large_bounds = renderer.destination_bounds(large_geometry)

    small_span = small_bounds[2] - small_bounds[0]
    large_span = large_bounds[2] - large_bounds[0]

    assert small_span > 1.7
    assert large_span == pytest.approx(8.4)


def test_destination_bounds_are_shifted_inside_source_coverage(settings):
    """Test destination bounds are shifted inside source coverage."""
    renderer = AreaRenderer(settings)

    class _FakeSource:
        width = 100
        height = 100
        transform = Affine(0.1, 0.0, -75.0, 0.0, -0.1, -25.0)
        crs = "EPSG:4326"

    fitted = renderer._fit_destination_bounds_to_source(
        (-54.2, -35.7, -52.5, -34.0),
        _FakeSource(),
    )

    assert fitted[1] >= -35.0
    assert fitted[3] <= -25.0
    assert fitted[2] - fitted[0] == pytest.approx(1.7)
    assert fitted[3] - fitted[1] == pytest.approx(1.7)


def test_renderer_falls_back_when_source_returns_empty_image(settings, monkeypatch):
    """Test renderer falls back when source returns empty image."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    area = catalog.get("municipio-3205309")
    assert area is not None

    monkeypatch.setattr(
        "goes_timelapse.pipeline.rendering._read_crop",
        lambda path, plan: Image.new("RGBA", (0, 0)),
    )

    renderer = AreaRenderer(settings)
    png_path = settings.processed_dir / area.area_id / "empty-fallback.png"
    png_path.parent.mkdir(parents=True, exist_ok=True)
    plan = RenderPlan(
        window=_window(0, 0, 1, 1),
        dst_bounds=(-41.0, -21.0, -39.0, -19.0),
        scaled_polygon=(),
        scaled_state_lines=(),
        output_size=(320, 320),
    )

    renderer._render_frame(
        FrameSpec(timestamp="20260011220", primary_path=Path("dummy.tif")),
        png_path,
        area,
        plan,
    )

    with Image.open(png_path) as result:
        assert result.size == (320, 320)


def test_renderer_blends_primary_and_secondary_frames(settings, monkeypatch):
    """Test renderer blends primary and secondary frames."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    area = catalog.get("municipio-3205309")
    assert area is not None

    renderer = AreaRenderer(settings)
    png_path = settings.processed_dir / area.area_id / "blended-frame.png"
    png_path.parent.mkdir(parents=True, exist_ok=True)
    plan = RenderPlan(
        window=_window(0, 0, 1, 1),
        dst_bounds=(-41.0, -21.0, -39.0, -19.0),
        scaled_polygon=(),
        scaled_state_lines=(),
        output_size=(40, 40),
    )

    def fake_load(raw_path: Path, area_id: str, render_plan: RenderPlan) -> Image.Image:
        color = (255, 0, 0, 255) if "primary" in raw_path.name else (0, 0, 255, 255)
        return Image.new("RGBA", render_plan.output_size, color)

    monkeypatch.setattr(renderer, "_load_frame_image", fake_load)
    monkeypatch.setattr(renderer, "_draw_overlay", lambda *args, **kwargs: None)

    renderer._render_frame(
        FrameSpec(
            timestamp="20260011220",
            primary_path=Path("20260011220_primary.tif"),
            blend_path=Path("20260011220_secondary.tif"),
            blend_alpha=0.25,
        ),
        png_path,
        area,
        plan,
    )

    with Image.open(png_path) as result:
        pixel = result.getpixel((20, 20))
        assert pixel == (191, 0, 63)


def test_renderer_draws_marker_when_present_in_render_plan(settings, monkeypatch):
    """Test renderer draws marker when present in render plan."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    area = catalog.get("municipio-3205309")
    assert area is not None

    renderer = AreaRenderer(settings)
    png_path = settings.processed_dir / area.area_id / "marker-frame.png"
    png_path.parent.mkdir(parents=True, exist_ok=True)
    plan = RenderPlan(
        window=_window(0, 0, 1, 1),
        dst_bounds=(-41.0, -21.0, -39.0, -19.0),
        scaled_polygon=(),
        scaled_state_lines=(),
        output_size=(80, 80),
        scaled_marker=(40, 40),
    )

    monkeypatch.setattr(
        renderer,
        "_load_frame_image",
        lambda raw_path, area_id, render_plan: Image.new(
            "RGBA", render_plan.output_size, (0, 0, 0, 255)
        ),
    )
    monkeypatch.setattr(renderer, "_draw_overlay", lambda *args, **kwargs: None)

    renderer._render_frame(
        FrameSpec(
            timestamp="20260011220",
            primary_path=Path(
                "OR_ABI-L1b-RadF-M6C02_G19_s20260011220200_e20260011229515_c20260011229547.tif"
            ),
        ),
        png_path,
        area,
        plan,
    )

    with Image.open(png_path) as result:
        pixel = cast(
            tuple[int, int, int, int], result.convert("RGBA").getpixel((40, 40))
        )
        assert pixel[:3] == (220, 42, 42)


def test_renderer_draws_lightning_points_when_present_in_frame_spec(
    settings, monkeypatch
):
    """Test renderer draws lightning points when present in frame spec."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    area = catalog.get("municipio-3205309")
    assert area is not None

    renderer = AreaRenderer(settings)
    png_path = settings.processed_dir / area.area_id / "lightning-frame.png"
    png_path.parent.mkdir(parents=True, exist_ok=True)
    plan = RenderPlan(
        window=_window(0, 0, 1, 1),
        dst_bounds=(-41.0, -21.0, -39.0, -19.0),
        scaled_polygon=(),
        scaled_state_lines=(),
        output_size=(80, 80),
    )

    monkeypatch.setattr(
        renderer,
        "_load_frame_image",
        lambda raw_path, area_id, render_plan: Image.new(
            "RGBA", render_plan.output_size, (0, 0, 0, 255)
        ),
    )
    monkeypatch.setattr(renderer, "_draw_overlay", lambda *args, **kwargs: None)

    renderer._render_frame(
        FrameSpec(
            timestamp="20260011220",
            primary_path=Path(
                "OR_ABI-L1b-RadF-M6C02_G19_s20260011220200_e20260011229515_c20260011229547.tif"
            ),
            lightning_points=((-40.0, -20.0),),
        ),
        png_path,
        area,
        plan,
    )

    with Image.open(png_path) as result:
        pixel = cast(
            tuple[int, int, int, int], result.convert("RGBA").getpixel((40, 40))
        )
        assert pixel[:3] == (42, 121, 255)


def test_process_frames_reuses_existing_pngs_for_unchanged_frame_specs(
    settings, monkeypatch
):
    """Test process frames reuses existing pngs for unchanged frame specs."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    area = catalog.get("municipio-3205309")
    assert area is not None
    geometry = IbgeGeometryStore(
        settings.geometry_cache_dir,
        base_url=settings.ibge_malhas_url,
        timeout_seconds=settings.ibge_request_timeout,
    ).load_geometry(area)

    tif_paths = [
        create_test_tif(settings.raw_dir / "20260011220_frame.tif", offset=0),
        create_test_tif(settings.raw_dir / "20260011230_frame.tif", offset=5),
    ]

    renderer = AreaRenderer(settings)
    render_calls: list[str] = []

    def fake_render(frame_spec, png_path, area_arg, plan):
        render_calls.append(frame_spec.timestamp)
        Image.new("RGB", (8, 8), (32, 32, 32)).save(png_path, format="PNG")

    monkeypatch.setattr(renderer, "_render_frame", fake_render)

    first_paths = renderer.process_frames(area, geometry, tif_paths)
    second_paths = renderer.process_frames(area, geometry, tif_paths)

    assert render_calls == ["20260011220", "20260011230"]
    assert first_paths == second_paths
    assert all(path.exists() for path in second_paths)


def test_process_frames_renders_only_new_frame_when_sequence_slides(
    settings, monkeypatch
):
    """Test process frames renders only new frame when sequence slides."""
    adjusted_settings = replace(settings, frame_count=3)
    catalog = AreaCatalog.from_path(adjusted_settings.catalog_path)
    area = catalog.get("municipio-3205309")
    assert area is not None
    geometry = IbgeGeometryStore(
        adjusted_settings.geometry_cache_dir,
        base_url=adjusted_settings.ibge_malhas_url,
        timeout_seconds=adjusted_settings.ibge_request_timeout,
    ).load_geometry(area)

    tif_paths = [
        create_test_tif(adjusted_settings.raw_dir / "20260011220_frame.tif", offset=0),
        create_test_tif(adjusted_settings.raw_dir / "20260011230_frame.tif", offset=5),
        create_test_tif(adjusted_settings.raw_dir / "20260011240_frame.tif", offset=10),
        create_test_tif(adjusted_settings.raw_dir / "20260011250_frame.tif", offset=15),
    ]

    renderer = AreaRenderer(adjusted_settings)
    render_calls: list[str] = []

    def fake_render(frame_spec, png_path, area_arg, plan):
        render_calls.append(frame_spec.timestamp)
        Image.new("RGB", (8, 8), (64, 64, 64)).save(png_path, format="PNG")

    monkeypatch.setattr(renderer, "_render_frame", fake_render)

    first_paths = renderer.process_frames(area, geometry, tif_paths[:3])
    second_paths = renderer.process_frames(area, geometry, tif_paths[1:])

    assert render_calls == ["20260011220", "20260011230", "20260011240", "20260011250"]
    assert [path.name for path in second_paths] != [path.name for path in first_paths]
    assert not first_paths[0].exists()
    assert all(path.exists() for path in second_paths)


def test_process_frames_rerenders_when_blend_alpha_changes(settings, monkeypatch):
    """Test process frames rerenders when blend alpha changes."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    area = catalog.get("municipio-3205309")
    assert area is not None
    geometry = IbgeGeometryStore(
        settings.geometry_cache_dir,
        base_url=settings.ibge_malhas_url,
        timeout_seconds=settings.ibge_request_timeout,
    ).load_geometry(area)

    primary_path = create_test_tif(
        settings.raw_dir / "20260011220_primary.tif", offset=0
    )
    blend_path = create_test_tif(settings.raw_dir / "20260011220_blend.tif", offset=5)
    renderer = AreaRenderer(settings)
    render_calls: list[str] = []

    def fake_render(frame_spec, png_path, area_arg, plan):
        render_calls.append(f"{frame_spec.timestamp}:{frame_spec.blend_alpha}")
        Image.new("RGB", (8, 8), (96, 96, 96)).save(png_path, format="PNG")

    monkeypatch.setattr(renderer, "_render_frame", fake_render)

    renderer.process_frames(
        area,
        geometry,
        [
            FrameSpec(
                timestamp="20260011220",
                primary_path=primary_path,
                blend_path=blend_path,
                blend_alpha=0.2,
            )
        ],
    )
    renderer.process_frames(
        area,
        geometry,
        [
            FrameSpec(
                timestamp="20260011220",
                primary_path=primary_path,
                blend_path=blend_path,
                blend_alpha=0.6,
            )
        ],
    )

    assert render_calls == ["20260011220:0.2", "20260011220:0.6"]


def test_process_frames_rerenders_when_render_plan_changes(settings, monkeypatch):
    """Test process frames rerenders when render plan changes."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    area = catalog.get("municipio-3205309")
    assert area is not None
    geometry = IbgeGeometryStore(
        settings.geometry_cache_dir,
        base_url=settings.ibge_malhas_url,
        timeout_seconds=settings.ibge_request_timeout,
    ).load_geometry(area)

    tif_path = create_test_tif(settings.raw_dir / "20260011220_frame.tif", offset=0)
    renderer = AreaRenderer(settings)
    render_calls: list[str] = []
    plans = [
        RenderPlan(
            window=_window(0, 0, 1, 1),
            dst_bounds=(-41.0, -21.0, -39.0, -19.0),
            scaled_polygon=(),
            scaled_state_lines=(),
            output_size=(320, 320),
        ),
        RenderPlan(
            window=_window(0, 0, 1, 1),
            dst_bounds=(-42.0, -22.0, -38.0, -18.0),
            scaled_polygon=(),
            scaled_state_lines=(),
            output_size=(360, 360),
        ),
    ]

    class _FakeSource:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

    def fake_render(frame_spec, png_path, area_arg, plan):
        render_calls.append(f"{frame_spec.timestamp}:{plan.output_size[0]}")
        Image.new("RGB", (8, 8), (128, 128, 128)).save(png_path, format="PNG")

    monkeypatch.setattr(
        "goes_timelapse.pipeline.rendering.rasterio.open", lambda path: _FakeSource()
    )
    monkeypatch.setattr(
        renderer,
        "_build_render_plan",
        lambda geometry_arg, src, marker_coordinates=None: plans.pop(0),
    )
    monkeypatch.setattr(renderer, "_render_frame", fake_render)

    renderer.process_frames(area, geometry, [tif_path])
    renderer.process_frames(area, geometry, [tif_path])

    assert render_calls == ["20260011220:320", "20260011220:360"]


def test_process_frames_rerenders_when_marker_changes(settings, monkeypatch):
    """Test process frames rerenders when marker changes."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    area = catalog.get("municipio-3205309")
    assert area is not None
    geometry = IbgeGeometryStore(
        settings.geometry_cache_dir,
        base_url=settings.ibge_malhas_url,
        timeout_seconds=settings.ibge_request_timeout,
    ).load_geometry(area)

    tif_path = create_test_tif(settings.raw_dir / "20260011220_frame.tif", offset=0)
    renderer = AreaRenderer(settings)
    render_calls: list[str] = []

    def fake_render(frame_spec, png_path, area_arg, plan):
        render_calls.append(
            f"{frame_spec.timestamp}:{round(plan.scaled_marker[0], 1) if plan.scaled_marker else 'none'}"
        )
        Image.new("RGB", (8, 8), (160, 160, 160)).save(png_path, format="PNG")

    monkeypatch.setattr(renderer, "_render_frame", fake_render)

    renderer.process_frames(area, geometry, [tif_path], marker_coordinates=None)
    renderer.process_frames(
        area, geometry, [tif_path], marker_coordinates=(-40.29, -20.32)
    )

    assert render_calls[0] == "20260011220:none"
    assert render_calls[1].startswith("20260011220:")
    assert render_calls[1] != "20260011220:none"


def test_process_frames_rerenders_when_lightning_points_change(settings, monkeypatch):
    """Test process frames rerenders when lightning points change."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    area = catalog.get("municipio-3205309")
    assert area is not None
    geometry = IbgeGeometryStore(
        settings.geometry_cache_dir,
        base_url=settings.ibge_malhas_url,
        timeout_seconds=settings.ibge_request_timeout,
    ).load_geometry(area)

    tif_path = create_test_tif(settings.raw_dir / "20260011220_frame.tif", offset=0)
    renderer = AreaRenderer(settings)
    render_calls: list[str] = []

    def fake_render(frame_spec, png_path, area_arg, plan):
        render_calls.append(
            f"{frame_spec.timestamp}:{len(frame_spec.lightning_points)}"
        )
        Image.new("RGB", (8, 8), (180, 180, 180)).save(png_path, format="PNG")

    monkeypatch.setattr(renderer, "_render_frame", fake_render)

    renderer.process_frames(
        area,
        geometry,
        [
            FrameSpec(
                timestamp="20260011220", primary_path=tif_path, lightning_points=()
            )
        ],
    )
    renderer.process_frames(
        area,
        geometry,
        [
            FrameSpec(
                timestamp="20260011220",
                primary_path=tif_path,
                lightning_points=((-40.29, -20.32),),
            )
        ],
    )

    assert render_calls == ["20260011220:0", "20260011220:1"]
