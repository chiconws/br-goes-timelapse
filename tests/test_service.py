"""Tests for the refresh service."""

from __future__ import annotations

import asyncio
from dataclasses import replace
import json
import sqlite3
from types import SimpleNamespace

import pytest

from goes_timelapse.core.state import StateStore
from goes_timelapse.data.catalog import AreaCatalog
from goes_timelapse.pipeline import service as service_module
from goes_timelapse.pipeline.downloader import DownloadReport
from goes_timelapse.pipeline.rendering import FrameSpec
from goes_timelapse.pipeline.service import (
    RAW_SOURCE_INFRARED,
    RAW_SOURCE_LIGHTNING,
    RAW_SOURCE_VISIBLE,
    DownloadSourcePlan,
    GoesTimelapseService,
    StorageCheck,
)
from goes_timelapse.timeline import (
    PHASE_SUNRISE_BLEND,
    PHASE_SUNSET_BLEND,
    PHASE_VISIBLE,
    SOURCE_INFRARED,
    SOURCE_LIGHTNING,
    SOURCE_VISIBLE,
    AreaTimelinePlan,
    TimelineFrame,
)


def _plan_for_slots(
    area_id: str,
    *frames: tuple[str, str, str, str | None, float | None, tuple[str, ...]],
) -> AreaTimelinePlan:
    return AreaTimelinePlan(
        area_id=area_id,
        frames=tuple(
            TimelineFrame(
                slot_timestamp=slot_timestamp,
                phase=phase,
                primary_source=primary_source,
                blend_source=blend_source,
                blend_alpha=blend_alpha,
                required_sources=required_sources,
            )
            for slot_timestamp, phase, primary_source, blend_source, blend_alpha, required_sources in frames
        ),
    )


def _target_map(
    *,
    visible: tuple[str, ...] = (),
    infrared: tuple[str, ...] = (),
    lightning: tuple[str, ...] = (),
) -> dict[str, tuple[str, ...]]:
    return {
        RAW_SOURCE_VISIBLE: visible,
        RAW_SOURCE_INFRARED: infrared,
        RAW_SOURCE_LIGHTNING: lightning,
    }


@pytest.mark.asyncio
async def test_adding_first_tracked_area_triggers_immediate_refresh(
    settings, monkeypatch
):
    """Test adding first tracked area triggers immediate refresh."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    state_store = StateStore(settings.db_path)
    service = GoesTimelapseService(
        settings=settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=True,
    )

    calls: list[str] = []

    async def fake_refresh():
        calls.append("refresh")

    monkeypatch.setattr(service, "refresh_raw_frames", fake_refresh)

    await service.add_tracked("municipio-3205309")
    await asyncio.sleep(0)

    assert calls == ["refresh"]
    state_store.close()


@pytest.mark.asyncio
async def test_build_download_plans_uses_visible_only_during_day(settings, monkeypatch):
    """Test build download plans uses visible only during day."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    state_store = StateStore(settings.db_path)
    service = GoesTimelapseService(
        settings=settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )
    area = catalog.get("municipio-3205309")
    assert area is not None
    state_store.upsert_tracked(area, status="ready")

    monkeypatch.setattr(
        service,
        "_build_global_target_timestamps",
        lambda tracked_areas, reference_moment=None: _target_map(
            visible=("20260701820", "20260701810", "20260701800"),
            lightning=("20260701820", "20260701810", "20260701800"),
        ),
    )

    plans = await service._build_download_plans()

    visible = next(plan for plan in plans if plan.source_key == RAW_SOURCE_VISIBLE)
    infrared = next(plan for plan in plans if plan.source_key == RAW_SOURCE_INFRARED)
    lightning = next(plan for plan in plans if plan.source_key == RAW_SOURCE_LIGHTNING)
    assert visible.should_download is True
    assert infrared.should_download is False
    assert lightning.should_download is True
    assert visible.target_timestamps == ("20260701820", "20260701810", "20260701800")
    assert infrared.target_timestamps == ()
    assert lightning.reason == "Ativo em 3 slots úteis de GLM na timeline atual"
    state_store.close()


@pytest.mark.asyncio
async def test_build_download_plans_keeps_lightning_independent_from_imagery_state(
    settings, monkeypatch
):
    """Test build download plans keeps lightning independent from imagery state."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    state_store = StateStore(settings.db_path)
    service = GoesTimelapseService(
        settings=settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )
    area = catalog.get("municipio-3205309")
    assert area is not None
    state_store.upsert_tracked(area, status="ready")

    monkeypatch.setattr(
        service,
        "_build_global_target_timestamps",
        lambda tracked_areas, reference_moment=None: _target_map(
            infrared=("20260701820", "20260701810", "20260701800"),
            lightning=("20260701820", "20260701810", "20260701800"),
        ),
    )

    plans = await service._build_download_plans()

    visible = next(plan for plan in plans if plan.source_key == RAW_SOURCE_VISIBLE)
    infrared = next(plan for plan in plans if plan.source_key == RAW_SOURCE_INFRARED)
    lightning = next(plan for plan in plans if plan.source_key == RAW_SOURCE_LIGHTNING)
    assert visible.should_download is False
    assert infrared.should_download is True
    assert lightning.should_download is True
    assert lightning.target_timestamps == ("20260701820", "20260701810", "20260701800")
    assert lightning.reason == "Ativo em 3 slots úteis de GLM na timeline atual"
    state_store.close()


def test_process_area_continues_when_processing_status_write_fails(
    settings, monkeypatch
):
    """Test process area continues when processing status write fails."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    state_store = StateStore(settings.db_path)
    service = GoesTimelapseService(
        settings=settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )
    area = catalog.get("municipio-3205309")
    assert area is not None
    state_store.upsert_tracked(area, status="queued")

    frame_path = (
        settings.raw_dir
        / "visible"
        / "OR_ABI-L1b-RadF-M6C02_G19_s2026070180000_e2026070180952_c2026070180956.tif"
    )
    frame_path.parent.mkdir(parents=True, exist_ok=True)
    frame_path.write_bytes(b"frame")

    png_path = settings.processed_dir / area.area_id / "frame.png"
    png_path.parent.mkdir(parents=True, exist_ok=True)
    png_path.write_bytes(b"png")

    media_path = settings.media_dir / f"{area.area_id}.webp"
    snippet_path = settings.snippets_dir / f"{area.area_id}.yaml"

    monkeypatch.setattr(
        service,
        "_build_area_timeline_plan",
        lambda area_obj, reference_moment=None: _plan_for_slots(
            area_obj.area_id,
            (
                "20260701800",
                PHASE_VISIBLE,
                SOURCE_VISIBLE,
                None,
                None,
                (SOURCE_VISIBLE, SOURCE_LIGHTNING),
            ),
        ),
    )
    monkeypatch.setattr(
        service,
        "_build_frame_specs",
        lambda area_obj: [FrameSpec(timestamp="20260701800", primary_path=frame_path)],
    )
    monkeypatch.setattr(
        service.geometry_store,
        "load_geometry",
        lambda area_obj: SimpleNamespace(
            area_id=area_obj.area_id,
            centroid=(-40.3, -20.3),
            bounds=(-40.4, -20.4, -40.2, -20.2),
            polygon=(
                (-40.4, -20.4),
                (-40.2, -20.4),
                (-40.2, -20.2),
                (-40.4, -20.2),
                (-40.4, -20.4),
            ),
        ),
    )
    monkeypatch.setattr(
        service.renderer, "process_frames", lambda *args, **kwargs: [png_path]
    )
    monkeypatch.setattr(
        service.webp_builder, "build", lambda *args, **kwargs: media_path
    )
    monkeypatch.setattr(
        service_module, "write_lovelace_snippet", lambda *args, **kwargs: snippet_path
    )

    original_set_status = state_store.set_status

    def flaky_set_status(area_id, status, **kwargs):
        if status == "processing":
            raise sqlite3.OperationalError("disk I/O error")
        return original_set_status(area_id, status, **kwargs)

    monkeypatch.setattr(state_store, "set_status", flaky_set_status)

    rendered = service._process_area(area.area_id)
    tracked = state_store.get_tracked(area.area_id)
    state_store.close()

    assert rendered is not None
    assert tracked is not None
    assert tracked.status == "ready"


def test_available_raw_paths_include_nonempty_files_without_opening_raster(
    settings, monkeypatch
):
    """Test available raw paths include nonempty files without opening raster."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    state_store = StateStore(settings.db_path)
    service = GoesTimelapseService(
        settings=settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )

    raw_path = (
        settings.raw_dir
        / RAW_SOURCE_VISIBLE
        / "municipio-3205309"
        / "OR_ABI-L1b-RadF-M6C02_G19_s2026070180000_e2026070180952_c2026070180956.tif"
    )
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_bytes(b"not-a-real-geotiff-but-nonempty")

    available = service._available_raw_paths_by_source("municipio-3205309")
    state_store.close()

    assert available[RAW_SOURCE_VISIBLE] == {"20260701800": raw_path}


def test_status_snapshot_reports_staging_storage_pressure(settings):
    """Test status snapshot reports staging storage pressure."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    state_store = StateStore(settings.db_path)
    service = GoesTimelapseService(
        settings=settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )

    service._storage_checks = lambda: {
        "cache": StorageCheck(
            key="cache",
            label="Cache bruto",
            path=settings.data_dir,
            free_bytes=30 * 1024 * 1024 * 1024,
            total_bytes=100 * 1024 * 1024 * 1024,
            warning=None,
            is_blocking=False,
        ),
        "source": StorageCheck(
            key="source",
            label="Staging de download",
            path=settings.source_dir,
            free_bytes=9 * 1024 * 1024 * 1024,
            total_bytes=20 * 1024 * 1024 * 1024,
            warning=None,
            is_blocking=False,
        ),
        "scratch": StorageCheck(
            key="scratch",
            label="Scratch de conversão",
            path=settings.scratch_dir,
            free_bytes=512 * 1024 * 1024,
            total_bytes=20 * 1024 * 1024 * 1024,
            warning=(
                f"Scratch de conversão em '{settings.scratch_dir}' "
                "sem espaço suficiente (512.0 MB livres)"
            ),
            is_blocking=True,
        ),
    }

    snapshot = service.status_snapshot()
    state_store.close()

    assert snapshot["disk_free_bytes"] == 30 * 1024 * 1024 * 1024
    assert snapshot["staging_free_bytes"] == 512 * 1024 * 1024
    assert snapshot["staging_path"] == str(settings.scratch_dir)
    assert "Scratch de conversão" in str(snapshot["disk_warning"])


def test_process_area_rebuilds_existing_media_when_frames_are_available(
    settings, monkeypatch
):
    """Test process area rebuilds existing media when frames are available."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    state_store = StateStore(settings.db_path)
    service = GoesTimelapseService(
        settings=settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )
    area = catalog.get("municipio-3205309")
    assert area is not None
    state_store.upsert_tracked(area, status="ready")

    media_path = settings.media_dir / f"{area.area_id}.webp"
    media_path.parent.mkdir(parents=True, exist_ok=True)
    media_path.write_bytes(b"existing-media")
    state_store.set_status(
        area.area_id,
        "ready",
        media_path=str(media_path),
        latest_source_timestamp="20260701850",
    )

    frame_path = (
        settings.raw_dir
        / "visible"
        / "OR_ABI-L1b-RadF-M6C02_G19_s2026070185000_e2026070185952_c2026070185956.tif"
    )
    frame_path.parent.mkdir(parents=True, exist_ok=True)
    frame_path.write_bytes(b"frame")

    monkeypatch.setattr(
        service,
        "_build_frame_specs",
        lambda area_obj: [
            FrameSpec(timestamp="20260701830", primary_path=frame_path),
            FrameSpec(timestamp="20260701840", primary_path=frame_path),
            FrameSpec(timestamp="20260701850", primary_path=frame_path),
        ],
    )
    png_path = settings.processed_dir / area.area_id / "frame.png"
    png_path.parent.mkdir(parents=True, exist_ok=True)
    png_path.write_bytes(b"png")
    snippet_path = settings.snippets_dir / f"{area.area_id}.yaml"
    monkeypatch.setattr(
        service.geometry_store,
        "load_geometry",
        lambda area_obj: SimpleNamespace(
            area_id=area_obj.area_id,
            centroid=(-40.3, -20.3),
            bounds=(-40.4, -20.4, -40.2, -20.2),
            polygon=(
                (-40.4, -20.4),
                (-40.2, -20.4),
                (-40.2, -20.2),
                (-40.4, -20.2),
                (-40.4, -20.4),
            ),
        ),
    )
    monkeypatch.setattr(
        service.renderer, "process_frames", lambda *args, **kwargs: [png_path]
    )
    monkeypatch.setattr(
        service.webp_builder, "build", lambda *args, **kwargs: media_path
    )
    monkeypatch.setattr(
        service_module, "write_lovelace_snippet", lambda *args, **kwargs: snippet_path
    )

    rendered = service._process_area(area.area_id)
    tracked = state_store.get_tracked(area.area_id)
    state_store.close()

    assert rendered is not None
    assert tracked is not None
    assert tracked.status == "ready"
    assert tracked.media_path == str(media_path)
    assert tracked.latest_source_timestamp == "20260701850"


def test_process_area_renders_new_area_after_bootstrap_minimum(settings, monkeypatch):
    """Test process area renders new area after bootstrap minimum."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    state_store = StateStore(settings.db_path)
    service = GoesTimelapseService(
        settings=settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )
    area = catalog.get("municipio-3205309")
    assert area is not None
    state_store.upsert_tracked(area, status="queued")

    frame_path = (
        settings.raw_dir
        / "visible"
        / "OR_ABI-L1b-RadF-M6C02_G19_s2026070185000_e2026070185952_c2026070185956.tif"
    )
    frame_path.parent.mkdir(parents=True, exist_ok=True)
    frame_path.write_bytes(b"frame")

    png_path = settings.processed_dir / area.area_id / "frame.png"
    png_path.parent.mkdir(parents=True, exist_ok=True)
    png_path.write_bytes(b"png")

    media_path = settings.media_dir / f"{area.area_id}.webp"
    snippet_path = settings.snippets_dir / f"{area.area_id}.yaml"

    monkeypatch.setattr(
        service,
        "_build_area_timeline_plan",
        lambda area_obj, reference_moment=None: _plan_for_slots(
            area_obj.area_id,
            (
                "20260701810",
                PHASE_VISIBLE,
                SOURCE_VISIBLE,
                None,
                None,
                (SOURCE_VISIBLE, SOURCE_LIGHTNING),
            ),
            (
                "20260701820",
                PHASE_VISIBLE,
                SOURCE_VISIBLE,
                None,
                None,
                (SOURCE_VISIBLE, SOURCE_LIGHTNING),
            ),
            (
                "20260701830",
                PHASE_VISIBLE,
                SOURCE_VISIBLE,
                None,
                None,
                (SOURCE_VISIBLE, SOURCE_LIGHTNING),
            ),
            (
                "20260701840",
                PHASE_VISIBLE,
                SOURCE_VISIBLE,
                None,
                None,
                (SOURCE_VISIBLE, SOURCE_LIGHTNING),
            ),
            (
                "20260701850",
                PHASE_VISIBLE,
                SOURCE_VISIBLE,
                None,
                None,
                (SOURCE_VISIBLE, SOURCE_LIGHTNING),
            ),
        ),
    )
    monkeypatch.setattr(
        service,
        "_build_frame_specs",
        lambda area_obj: [
            FrameSpec(timestamp="20260701830", primary_path=frame_path),
            FrameSpec(timestamp="20260701840", primary_path=frame_path),
            FrameSpec(timestamp="20260701850", primary_path=frame_path),
        ],
    )
    monkeypatch.setattr(
        service.geometry_store,
        "load_geometry",
        lambda area_obj: SimpleNamespace(
            area_id=area_obj.area_id,
            centroid=(-40.3, -20.3),
            bounds=(-40.4, -20.4, -40.2, -20.2),
            polygon=(
                (-40.4, -20.4),
                (-40.2, -20.4),
                (-40.2, -20.2),
                (-40.4, -20.2),
                (-40.4, -20.4),
            ),
        ),
    )
    monkeypatch.setattr(
        service.renderer, "process_frames", lambda *args, **kwargs: [png_path]
    )
    monkeypatch.setattr(
        service.webp_builder, "build", lambda *args, **kwargs: media_path
    )
    monkeypatch.setattr(
        service_module, "write_lovelace_snippet", lambda *args, **kwargs: snippet_path
    )

    rendered = service._process_area(area.area_id)
    tracked = state_store.get_tracked(area.area_id)
    state_store.close()

    assert rendered is not None
    assert tracked is not None
    assert tracked.status == "ready"


@pytest.mark.asyncio
async def test_worker_loop_survives_status_persistence_failure_in_error_handler(
    settings,
    monkeypatch,
):
    """Test worker loop survives status persistence failure in error handler."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    state_store = StateStore(settings.db_path)
    service = GoesTimelapseService(
        settings=settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )
    area = catalog.get("municipio-3205309")
    assert area is not None
    state_store.upsert_tracked(area, status="queued")

    monkeypatch.setattr(
        service,
        "_process_area",
        lambda area_id: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(
        state_store,
        "set_status",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            sqlite3.OperationalError("disk I/O error")
        ),
    )

    worker_task = asyncio.create_task(service._worker_loop())
    await service.enqueue(area.area_id)
    await asyncio.wait_for(service._queue.join(), timeout=1)

    assert not worker_task.done()

    worker_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await worker_task
    state_store.close()


@pytest.mark.asyncio
async def test_build_download_plans_keeps_both_sources_active_during_sunrise_transition(
    settings, monkeypatch
):
    """Test build download plans keeps both sources active during sunrise transition."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    state_store = StateStore(settings.db_path)
    service = GoesTimelapseService(
        settings=settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )
    area = catalog.get("municipio-3205309")
    assert area is not None
    state_store.upsert_tracked(area, status="ready")

    monkeypatch.setattr(
        service,
        "_build_global_target_timestamps",
        lambda tracked_areas, reference_moment=None: _target_map(
            visible=("20260700700",),
            infrared=("20260700700", "20260700650"),
            lightning=("20260700700", "20260700650"),
        ),
    )

    plans = await service._build_download_plans()

    visible = next(plan for plan in plans if plan.source_key == RAW_SOURCE_VISIBLE)
    infrared = next(plan for plan in plans if plan.source_key == RAW_SOURCE_INFRARED)
    assert visible.should_download is True
    assert infrared.should_download is True
    assert visible.target_timestamps == ("20260700700",)
    assert infrared.target_timestamps == ("20260700700", "20260700650")
    state_store.close()


@pytest.mark.asyncio
async def test_build_download_plans_uses_infrared_only_at_night(settings, monkeypatch):
    """Test build download plans uses infrared only at night."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    state_store = StateStore(settings.db_path)
    service = GoesTimelapseService(
        settings=settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )
    area = catalog.get("municipio-3205309")
    assert area is not None
    state_store.upsert_tracked(area, status="ready")

    monkeypatch.setattr(
        service,
        "_build_global_target_timestamps",
        lambda tracked_areas, reference_moment=None: _target_map(
            infrared=("20260700650", "20260700640", "20260700630"),
            lightning=("20260700650", "20260700640", "20260700630"),
        ),
    )

    plans = await service._build_download_plans()

    visible = next(plan for plan in plans if plan.source_key == RAW_SOURCE_VISIBLE)
    infrared = next(plan for plan in plans if plan.source_key == RAW_SOURCE_INFRARED)
    assert visible.should_download is False
    assert infrared.should_download is True
    lightning = next(plan for plan in plans if plan.source_key == RAW_SOURCE_LIGHTNING)
    assert lightning.should_download is True
    assert infrared.target_timestamps == ("20260700650", "20260700640", "20260700630")
    assert lightning.reason == "Ativo em 3 slots úteis de GLM na timeline atual"
    state_store.close()


@pytest.mark.asyncio
async def test_build_download_plans_can_activate_visible_and_infrared_for_different_areas(
    settings, monkeypatch
):
    """Test build download plans can activate visible and infrared for different areas."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    state_store = StateStore(settings.db_path)
    service = GoesTimelapseService(
        settings=settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )
    area_a = catalog.get("municipio-3205309")
    area_b = catalog.get("municipio-3509502")
    assert area_a is not None
    assert area_b is not None
    state_store.upsert_tracked(area_a, status="ready")
    state_store.upsert_tracked(area_b, status="ready")

    monkeypatch.setattr(
        service,
        "_build_global_target_timestamps",
        lambda tracked_areas, reference_moment=None: _target_map(
            visible=("20260701820", "20260701810"),
            infrared=("20260701820", "20260701810"),
            lightning=("20260701820", "20260701810"),
        ),
    )

    plans = await service._build_download_plans()

    visible = next(plan for plan in plans if plan.source_key == RAW_SOURCE_VISIBLE)
    infrared = next(plan for plan in plans if plan.source_key == RAW_SOURCE_INFRARED)
    lightning = next(plan for plan in plans if plan.source_key == RAW_SOURCE_LIGHTNING)
    assert visible.should_download is True
    assert infrared.should_download is True
    assert lightning.should_download is True
    assert visible.target_timestamps == ("20260701820", "20260701810")
    assert infrared.target_timestamps == ("20260701820", "20260701810")
    state_store.close()


def test_build_frame_specs_blends_matching_visible_and_infrared_frames(
    settings, monkeypatch
):
    """Test build frame specs blends matching visible and infrared frames."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    state_store = StateStore(settings.db_path)
    service = GoesTimelapseService(
        settings=settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )
    area = catalog.get("municipio-3205309")
    assert area is not None

    visible_dir = settings.raw_dir / "visible" / area.area_id
    infrared_dir = settings.raw_dir / "infrared" / area.area_id
    visible_dir.mkdir(parents=True, exist_ok=True)
    infrared_dir.mkdir(parents=True, exist_ok=True)
    visible_path = (
        visible_dir
        / "OR_ABI-L1b-RadF-M6C02_G19_s2026070180000_e2026070180952_c2026070180956.tif"
    )
    infrared_path = (
        infrared_dir
        / "OR_ABI-L1b-RadF-M6C13_G19_s2026070180000_e2026070180952_c2026070180956.tif"
    )
    visible_path.write_bytes(b"visible")
    infrared_path.write_bytes(b"infrared")

    monkeypatch.setattr(service, "_is_valid_raw", lambda path: True)
    monkeypatch.setattr(
        service,
        "_build_area_timeline_plan",
        lambda area_obj, reference_moment=None: _plan_for_slots(
            area_obj.area_id,
            (
                "20260701800",
                PHASE_SUNSET_BLEND,
                SOURCE_VISIBLE,
                SOURCE_INFRARED,
                0.6,
                (SOURCE_VISIBLE, SOURCE_INFRARED, SOURCE_LIGHTNING),
            ),
        ),
    )

    specs = service._build_frame_specs(area)

    assert specs == [
        FrameSpec(
            timestamp="20260701800",
            primary_path=visible_path,
            blend_path=infrared_path,
            blend_alpha=0.6,
        )
    ]
    state_store.close()


def test_build_frame_specs_blends_matching_infrared_and_visible_frames_at_sunrise(
    settings, monkeypatch
):
    """Test build frame specs blends matching infrared and visible frames at sunrise."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    state_store = StateStore(settings.db_path)
    service = GoesTimelapseService(
        settings=settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )
    area = catalog.get("municipio-3205309")
    assert area is not None

    visible_dir = settings.raw_dir / "visible" / area.area_id
    infrared_dir = settings.raw_dir / "infrared" / area.area_id
    visible_dir.mkdir(parents=True, exist_ok=True)
    infrared_dir.mkdir(parents=True, exist_ok=True)
    visible_path = (
        visible_dir
        / "OR_ABI-L1b-RadF-M6C02_G19_s2026070070000_e2026070070952_c2026070070956.tif"
    )
    infrared_path = (
        infrared_dir
        / "OR_ABI-L1b-RadF-M6C13_G19_s2026070070000_e2026070070952_c2026070070956.tif"
    )
    visible_path.write_bytes(b"visible")
    infrared_path.write_bytes(b"infrared")

    monkeypatch.setattr(service, "_is_valid_raw", lambda path: True)
    monkeypatch.setattr(
        service,
        "_build_area_timeline_plan",
        lambda area_obj, reference_moment=None: _plan_for_slots(
            area_obj.area_id,
            (
                "20260700700",
                PHASE_SUNRISE_BLEND,
                SOURCE_INFRARED,
                SOURCE_VISIBLE,
                0.4,
                (SOURCE_INFRARED, SOURCE_VISIBLE, SOURCE_LIGHTNING),
            ),
        ),
    )

    specs = service._build_frame_specs(area)

    assert specs == [
        FrameSpec(
            timestamp="20260700700",
            primary_path=infrared_path,
            blend_path=visible_path,
            blend_alpha=0.4,
        )
    ]
    state_store.close()


def test_build_frame_specs_skips_transition_slot_when_blend_pair_is_missing(
    settings, monkeypatch
):
    """Test build frame specs skips transition slot when blend pair is missing."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    state_store = StateStore(settings.db_path)
    service = GoesTimelapseService(
        settings=settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )
    area = catalog.get("municipio-3205309")
    assert area is not None

    infrared_dir = settings.raw_dir / "infrared"
    infrared_dir.mkdir(parents=True, exist_ok=True)
    infrared_path = (
        infrared_dir
        / "OR_ABI-L1b-RadF-M6C13_G19_s2026070070000_e2026070070952_c2026070070956.tif"
    )
    infrared_path.write_bytes(b"infrared")

    monkeypatch.setattr(service, "_is_valid_raw", lambda path: True)
    monkeypatch.setattr(
        service,
        "_build_area_timeline_plan",
        lambda area_obj, reference_moment=None: _plan_for_slots(
            area_obj.area_id,
            (
                "20260700700",
                PHASE_SUNRISE_BLEND,
                SOURCE_INFRARED,
                SOURCE_VISIBLE,
                0.2,
                (SOURCE_INFRARED, SOURCE_VISIBLE, SOURCE_LIGHTNING),
            ),
        ),
    )

    specs = service._build_frame_specs(area)

    assert specs == []
    state_store.close()


def test_build_frame_specs_attaches_lightning_points_for_matching_timestamp(
    settings, monkeypatch
):
    """Test build frame specs attaches lightning points for matching timestamp."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    state_store = StateStore(settings.db_path)
    service = GoesTimelapseService(
        settings=settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )
    area = catalog.get("municipio-3205309")
    assert area is not None

    visible_dir = settings.raw_dir / "visible" / area.area_id
    lightning_dir = settings.raw_dir / "lightning"
    visible_dir.mkdir(parents=True, exist_ok=True)
    lightning_dir.mkdir(parents=True, exist_ok=True)
    visible_path = (
        visible_dir
        / "OR_ABI-L1b-RadF-M6C02_G19_s2026070180000_e2026070180952_c2026070180956.tif"
    )
    visible_path.write_bytes(b"visible")
    (lightning_dir / "20260701800_glm.json").write_text(
        json.dumps(
            {
                "slot_timestamp": "20260701800",
                "source_filenames": [
                    "OR_GLM-L2-LCFA_G19_s20260701800200_e20260701800400_c20260701800420.nc"
                ],
                "flashes": [
                    {"lon": -40.28, "lat": -20.31},
                    {"lon": -40.26, "lat": -20.29},
                ],
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(service, "_is_valid_raw", lambda path: True)
    monkeypatch.setattr(
        service,
        "_build_area_timeline_plan",
        lambda area_obj, reference_moment=None: _plan_for_slots(
            area_obj.area_id,
            (
                "20260701800",
                PHASE_VISIBLE,
                SOURCE_VISIBLE,
                None,
                None,
                (SOURCE_VISIBLE, SOURCE_LIGHTNING),
            ),
        ),
    )

    specs = service._build_frame_specs(area)

    assert specs == [
        FrameSpec(
            timestamp="20260701800",
            primary_path=visible_path,
            lightning_points=((-40.28, -20.31), (-40.26, -20.29)),
        )
    ]
    state_store.close()


def test_prune_raw_cache_discards_stale_lightning_slots_outside_global_history(
    settings, monkeypatch
):
    """Test prune raw cache discards stale lightning slots outside global history."""
    adjusted_settings = replace(settings, raw_history=2)
    catalog = AreaCatalog.from_path(adjusted_settings.catalog_path)
    state_store = StateStore(adjusted_settings.db_path)
    service = GoesTimelapseService(
        settings=adjusted_settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )
    area = catalog.get("municipio-3205309")
    assert area is not None
    state_store.upsert_tracked(area, status="ready")

    visible_dir = adjusted_settings.raw_dir / "visible"
    lightning_dir = adjusted_settings.raw_dir / "lightning"
    visible_dir.mkdir(parents=True, exist_ok=True)
    lightning_dir.mkdir(parents=True, exist_ok=True)
    for timestamp in ("20260701820", "20260701810", "20260701800"):
        (
            visible_dir
            / f"OR_ABI-L1b-RadF-M6C02_G19_s{timestamp}00_e{timestamp}52_c{timestamp}56.tif"
        ).write_bytes(b"visible")
        (lightning_dir / f"{timestamp}_glm.json").write_text(
            json.dumps(
                {
                    "slot_timestamp": timestamp,
                    "source_filenames": [],
                    "flashes": [{"lon": -40.3, "lat": -20.3}],
                }
            ),
            encoding="utf-8",
        )

    monkeypatch.setattr(
        service,
        "_build_global_keep_timestamps",
        lambda: _target_map(
            visible=("20260701820", "20260701810"),
            lightning=("20260701820", "20260701810"),
        ),
    )

    latest = service._prune_raw_cache()

    assert latest == "20260701820"
    assert sorted(path.name for path in lightning_dir.glob("*.json")) == [
        "20260701810_glm.json",
        "20260701820_glm.json",
    ]
    state_store.close()


def test_prune_raw_cache_removes_files_when_keep_set_is_empty(settings, monkeypatch):
    """Test prune raw cache removes files when keep set is empty."""
    adjusted_settings = replace(settings, raw_history=2)
    catalog = AreaCatalog.from_path(adjusted_settings.catalog_path)
    state_store = StateStore(adjusted_settings.db_path)
    service = GoesTimelapseService(
        settings=adjusted_settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )
    area = catalog.get("municipio-3205309")
    assert area is not None
    state_store.upsert_tracked(area, status="ready")

    visible_dir = adjusted_settings.raw_dir / "visible"
    visible_dir.mkdir(parents=True, exist_ok=True)
    newest = (
        visible_dir
        / "OR_ABI-L1b-RadF-M6C02_G19_s2026070182000_e2026070182052_c2026070182056.tif"
    )
    older = (
        visible_dir
        / "OR_ABI-L1b-RadF-M6C02_G19_s2026070181000_e2026070181052_c2026070181056.tif"
    )
    newest.write_bytes(b"visible")
    older.write_bytes(b"visible")

    monkeypatch.setattr(service, "_build_global_keep_timestamps", lambda: _target_map())

    latest = service._prune_raw_cache()

    assert latest is None
    assert list(visible_dir.glob("*.tif")) == []
    state_store.close()


def test_prune_raw_cache_uses_timeline_keep_set_for_visible_files(
    settings, monkeypatch
):
    """Test prune raw cache uses timeline keep set for visible files."""
    adjusted_settings = replace(settings, raw_history=3)
    catalog = AreaCatalog.from_path(adjusted_settings.catalog_path)
    state_store = StateStore(adjusted_settings.db_path)
    service = GoesTimelapseService(
        settings=adjusted_settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )

    visible_dir = adjusted_settings.raw_dir / "visible"
    visible_dir.mkdir(parents=True, exist_ok=True)
    source_visible_dir = adjusted_settings.source_dir / "visible"
    source_visible_dir.mkdir(parents=True, exist_ok=True)

    for timestamp in (
        "20260701850",
        "20260701840",
        "20260701830",
        "20260701820",
    ):
        visible_name = f"OR_ABI-L1b-RadF-M6C02_G19_s{timestamp}00_e{timestamp}52_c{timestamp}56.tif"
        source_name = visible_name.replace(".tif", ".nc")
        (visible_dir / visible_name).write_bytes(b"visible")
        (source_visible_dir / source_name).write_bytes(b"source")

    area = catalog.get("municipio-3205309")
    assert area is not None
    state_store.upsert_tracked(area, status="ready")
    monkeypatch.setattr(
        service,
        "_build_global_keep_timestamps",
        lambda: _target_map(visible=("20260701850", "20260701840", "20260701830")),
    )

    latest = service._prune_raw_cache()

    assert latest == "20260701850"
    assert sorted(path.name for path in visible_dir.glob("*.tif")) == [
        "OR_ABI-L1b-RadF-M6C02_G19_s2026070183000_e2026070183052_c2026070183056.tif",
        "OR_ABI-L1b-RadF-M6C02_G19_s2026070184000_e2026070184052_c2026070184056.tif",
        "OR_ABI-L1b-RadF-M6C02_G19_s2026070185000_e2026070185052_c2026070185056.tif",
    ]
    assert sorted(path.name for path in source_visible_dir.glob("*.nc")) == [
        "OR_ABI-L1b-RadF-M6C02_G19_s2026070183000_e2026070183052_c2026070183056.nc",
        "OR_ABI-L1b-RadF-M6C02_G19_s2026070184000_e2026070184052_c2026070184056.nc",
        "OR_ABI-L1b-RadF-M6C02_G19_s2026070185000_e2026070185052_c2026070185056.nc",
    ]
    state_store.close()


def test_build_frame_specs_uses_planned_slots_only_and_skips_missing_ones(
    settings, monkeypatch
):
    """Test build frame specs uses planned slots only and skips missing ones."""
    adjusted_settings = replace(settings, frame_count=5)
    catalog = AreaCatalog.from_path(adjusted_settings.catalog_path)
    state_store = StateStore(adjusted_settings.db_path)
    service = GoesTimelapseService(
        settings=adjusted_settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )
    area = catalog.get("municipio-3205309")
    assert area is not None

    visible_dir = adjusted_settings.raw_dir / "visible" / area.area_id
    visible_dir.mkdir(parents=True, exist_ok=True)
    filenames = [
        "OR_ABI-L1b-RadF-M6C02_G19_s2026070180020_e2026070180952_c2026070180956.tif",
        "OR_ABI-L1b-RadF-M6C02_G19_s2026070181020_e2026070181952_c2026070181956.tif",
        "OR_ABI-L1b-RadF-M6C02_G19_s2026070182020_e2026070182952_c2026070182956.tif",
        "OR_ABI-L1b-RadF-M6C02_G19_s2026070183020_e2026070183952_c2026070183956.tif",
        "OR_ABI-L1b-RadF-M6C02_G19_s2026070184020_e2026070184952_c2026070184956.tif",
        "OR_ABI-L1b-RadF-M6C02_G19_s2026070185020_e2026070185952_c2026070185956.tif",
    ]
    for filename in filenames:
        (visible_dir / filename).write_bytes(b"visible")

    monkeypatch.setattr(service, "_is_valid_raw", lambda path: True)
    monkeypatch.setattr(
        service,
        "_build_area_timeline_plan",
        lambda area_obj, reference_moment=None: _plan_for_slots(
            area_obj.area_id,
            (
                "20260701810",
                PHASE_VISIBLE,
                SOURCE_VISIBLE,
                None,
                None,
                (SOURCE_VISIBLE, SOURCE_LIGHTNING),
            ),
            (
                "20260701820",
                PHASE_VISIBLE,
                SOURCE_VISIBLE,
                None,
                None,
                (SOURCE_VISIBLE, SOURCE_LIGHTNING),
            ),
            (
                "20260701830",
                PHASE_VISIBLE,
                SOURCE_VISIBLE,
                None,
                None,
                (SOURCE_VISIBLE, SOURCE_LIGHTNING),
            ),
            (
                "20260701840",
                PHASE_VISIBLE,
                SOURCE_VISIBLE,
                None,
                None,
                (SOURCE_VISIBLE, SOURCE_LIGHTNING),
            ),
            (
                "20260701850",
                PHASE_VISIBLE,
                SOURCE_VISIBLE,
                None,
                None,
                (SOURCE_VISIBLE, SOURCE_LIGHTNING),
            ),
        ),
    )

    specs = service._build_frame_specs(area)

    assert [spec.timestamp for spec in specs] == [
        "20260701810",
        "20260701820",
        "20260701830",
        "20260701840",
        "20260701850",
    ]
    state_store.close()


def test_prune_raw_cache_keeps_global_night_history_and_drops_visible_day_frames(
    settings, monkeypatch
):
    """Test prune raw cache keeps global night history and drops visible day frames."""
    adjusted_settings = replace(settings, raw_history=2)
    catalog = AreaCatalog.from_path(adjusted_settings.catalog_path)
    state_store = StateStore(adjusted_settings.db_path)
    service = GoesTimelapseService(
        settings=adjusted_settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )
    area = catalog.get("municipio-3205309")
    assert area is not None
    state_store.upsert_tracked(area, status="ready")

    visible_dir = adjusted_settings.raw_dir / "visible"
    infrared_dir = adjusted_settings.raw_dir / "infrared"
    source_visible_dir = adjusted_settings.source_dir / "visible"
    source_infrared_dir = adjusted_settings.source_dir / "infrared"
    visible_dir.mkdir(parents=True, exist_ok=True)
    infrared_dir.mkdir(parents=True, exist_ok=True)
    source_visible_dir.mkdir(parents=True, exist_ok=True)
    source_infrared_dir.mkdir(parents=True, exist_ok=True)

    timestamps = ("20260701800", "20260701810", "20260701820")
    for timestamp in timestamps:
        visible_name = f"OR_ABI-L1b-RadF-M6C02_G19_s{timestamp}00_e{timestamp}52_c{timestamp}56.tif"
        infrared_name = f"OR_ABI-L1b-RadF-M6C13_G19_s{timestamp}00_e{timestamp}52_c{timestamp}56.tif"
        (visible_dir / visible_name).write_bytes(b"visible")
        (infrared_dir / infrared_name).write_bytes(b"infrared")
        (source_visible_dir / visible_name.replace(".tif", ".nc")).write_bytes(
            b"source-visible"
        )
        (source_infrared_dir / infrared_name.replace(".tif", ".nc")).write_bytes(
            b"source-infrared"
        )

    monkeypatch.setattr(
        service,
        "_build_global_keep_timestamps",
        lambda: _target_map(
            infrared=("20260701820", "20260701810"),
            lightning=("20260701820", "20260701810"),
        ),
    )

    latest = service._prune_raw_cache()

    assert latest == "20260701820"
    assert list(visible_dir.glob("*.tif")) == []
    assert sorted(path.name for path in infrared_dir.glob("*.tif")) == [
        "OR_ABI-L1b-RadF-M6C13_G19_s2026070181000_e2026070181052_c2026070181056.tif",
        "OR_ABI-L1b-RadF-M6C13_G19_s2026070182000_e2026070182052_c2026070182056.tif",
    ]
    assert list(source_visible_dir.glob("*.nc")) == []
    state_store.close()


def test_prune_raw_cache_keeps_global_day_history_and_drops_old_infrared_frames(
    settings, monkeypatch
):
    """Test prune raw cache keeps global day history and drops old infrared frames."""
    adjusted_settings = replace(settings, raw_history=2)
    catalog = AreaCatalog.from_path(adjusted_settings.catalog_path)
    state_store = StateStore(adjusted_settings.db_path)
    service = GoesTimelapseService(
        settings=adjusted_settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )
    area = catalog.get("municipio-3205309")
    assert area is not None
    state_store.upsert_tracked(area, status="ready")

    visible_dir = adjusted_settings.raw_dir / "visible"
    infrared_dir = adjusted_settings.raw_dir / "infrared"
    visible_dir.mkdir(parents=True, exist_ok=True)
    infrared_dir.mkdir(parents=True, exist_ok=True)

    timestamps = ("20260701800", "20260701810", "20260701820")
    for timestamp in timestamps:
        visible_name = f"OR_ABI-L1b-RadF-M6C02_G19_s{timestamp}00_e{timestamp}52_c{timestamp}56.tif"
        infrared_name = f"OR_ABI-L1b-RadF-M6C13_G19_s{timestamp}00_e{timestamp}52_c{timestamp}56.tif"
        (visible_dir / visible_name).write_bytes(b"visible")
        (infrared_dir / infrared_name).write_bytes(b"infrared")

    monkeypatch.setattr(
        service,
        "_build_global_keep_timestamps",
        lambda: _target_map(
            visible=("20260701820", "20260701810"),
            lightning=("20260701820", "20260701810"),
        ),
    )

    latest = service._prune_raw_cache()

    assert latest == "20260701820"
    assert sorted(path.name for path in visible_dir.glob("*.tif")) == [
        "OR_ABI-L1b-RadF-M6C02_G19_s2026070181000_e2026070181052_c2026070181056.tif",
        "OR_ABI-L1b-RadF-M6C02_G19_s2026070182000_e2026070182052_c2026070182056.tif",
    ]
    assert list(infrared_dir.glob("*.tif")) == []
    state_store.close()


def test_prune_raw_cache_keeps_transition_pair_within_global_history(
    settings, monkeypatch
):
    """Test prune raw cache keeps transition pair within global history."""
    adjusted_settings = replace(settings, raw_history=2)
    catalog = AreaCatalog.from_path(adjusted_settings.catalog_path)
    state_store = StateStore(adjusted_settings.db_path)
    service = GoesTimelapseService(
        settings=adjusted_settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )
    area = catalog.get("municipio-3205309")
    assert area is not None
    state_store.upsert_tracked(area, status="ready")

    visible_dir = adjusted_settings.raw_dir / "visible"
    infrared_dir = adjusted_settings.raw_dir / "infrared"
    visible_dir.mkdir(parents=True, exist_ok=True)
    infrared_dir.mkdir(parents=True, exist_ok=True)

    transition_timestamp = "20260700700"
    night_timestamp = "20260700650"
    (
        visible_dir
        / f"OR_ABI-L1b-RadF-M6C02_G19_s{transition_timestamp}00_e{transition_timestamp}52_c{transition_timestamp}56.tif"
    ).write_bytes(b"visible")
    (
        infrared_dir
        / f"OR_ABI-L1b-RadF-M6C13_G19_s{transition_timestamp}00_e{transition_timestamp}52_c{transition_timestamp}56.tif"
    ).write_bytes(b"infrared")
    (
        infrared_dir
        / f"OR_ABI-L1b-RadF-M6C13_G19_s{night_timestamp}00_e{night_timestamp}52_c{night_timestamp}56.tif"
    ).write_bytes(b"infrared")

    monkeypatch.setattr(
        service,
        "_build_global_keep_timestamps",
        lambda: _target_map(
            visible=(transition_timestamp,),
            infrared=(transition_timestamp, night_timestamp),
            lightning=(transition_timestamp, night_timestamp),
        ),
    )

    latest = service._prune_raw_cache()

    assert latest == transition_timestamp
    assert sorted(path.name for path in visible_dir.glob("*.tif")) == [
        "OR_ABI-L1b-RadF-M6C02_G19_s2026070070000_e2026070070052_c2026070070056.tif",
    ]
    assert sorted(path.name for path in infrared_dir.glob("*.tif")) == [
        "OR_ABI-L1b-RadF-M6C13_G19_s2026070065000_e2026070065052_c2026070065056.tif",
        "OR_ABI-L1b-RadF-M6C13_G19_s2026070070000_e2026070070052_c2026070070056.tif",
    ]
    state_store.close()


def test_prune_raw_cache_discards_slots_missing_from_timeline_keep_set(
    settings, monkeypatch
):
    """Test prune raw cache discards slots missing from timeline keep set."""
    adjusted_settings = replace(settings, raw_history=2)
    catalog = AreaCatalog.from_path(adjusted_settings.catalog_path)
    state_store = StateStore(adjusted_settings.db_path)
    service = GoesTimelapseService(
        settings=adjusted_settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )
    area = catalog.get("municipio-3205309")
    assert area is not None
    state_store.upsert_tracked(area, status="ready")

    visible_dir = adjusted_settings.raw_dir / "visible"
    infrared_dir = adjusted_settings.raw_dir / "infrared"
    visible_dir.mkdir(parents=True, exist_ok=True)
    infrared_dir.mkdir(parents=True, exist_ok=True)

    (
        infrared_dir
        / "OR_ABI-L1b-RadF-M6C13_G19_s2026070182000_e2026070182052_c2026070182056.tif"
    ).write_bytes(b"infrared")
    (
        visible_dir
        / "OR_ABI-L1b-RadF-M6C02_G19_s2026070181000_e2026070181052_c2026070181056.tif"
    ).write_bytes(b"visible")

    monkeypatch.setattr(
        service,
        "_build_global_keep_timestamps",
        lambda: _target_map(visible=("20260701810",)),
    )

    latest = service._prune_raw_cache()

    assert latest == "20260701810"
    assert sorted(path.name for path in visible_dir.glob("*.tif")) == [
        "OR_ABI-L1b-RadF-M6C02_G19_s2026070181000_e2026070181052_c2026070181056.tif",
    ]
    assert list(infrared_dir.glob("*.tif")) == []
    state_store.close()


@pytest.mark.asyncio
async def test_refresh_requeues_area_when_cache_is_newer_than_rendered_media(
    settings, monkeypatch
):
    """Test refresh requeues area when cache is newer than rendered media."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    state_store = StateStore(settings.db_path)
    service = GoesTimelapseService(
        settings=settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )

    area = catalog.get("municipio-3205309")
    assert area is not None
    state_store.upsert_tracked(area, status="ready")
    state_store.set_status(
        area.area_id,
        "ready",
        latest_source_timestamp="20260701810",
    )

    newest_raw = (
        settings.raw_dir
        / "visible"
        / ("OR_ABI-L1b-RadF-M6C02_G19_s2026070183020_e2026070183952_c2026070183956.tif")
    )
    newest_raw.parent.mkdir(parents=True, exist_ok=True)
    newest_raw.write_bytes(b"raw")

    async def fake_build_download_plans():
        return [
            DownloadSourcePlan(
                source_key=RAW_SOURCE_VISIBLE,
                source_label="Visível B2",
                tracked_area_ids=(area.area_id,),
                should_download=True,
                target_timestamps=("20260701830",),
                reason="Teste",
            )
        ]

    async def fake_refresh_latest():
        return DownloadReport(
            kept_files=[newest_raw],
            downloaded_count=0,
            attempted_count=1,
            failed_count=0,
            last_downloaded=None,
            latest_available=newest_raw.name,
        )

    queued_ids: list[str] = []

    async def fake_enqueue(area_id: str):
        queued_ids.append(area_id)

    monkeypatch.setattr(service, "_build_download_plans", fake_build_download_plans)
    monkeypatch.setattr(
        service._downloaders[RAW_SOURCE_VISIBLE], "refresh_latest", fake_refresh_latest
    )
    monkeypatch.setattr(service, "enqueue", fake_enqueue)
    monkeypatch.setattr(service, "_prune_raw_cache", lambda: "20260701830")

    await service.refresh_raw_frames()

    assert queued_ids == [area.area_id]
    tracked = state_store.get_tracked(area.area_id)
    assert tracked is not None
    assert tracked.status == "queued"
    state_store.close()


@pytest.mark.asyncio
async def test_refresh_raw_frames_prunes_using_runtime_cache_policy(
    settings, monkeypatch
):
    """Test refresh raw frames prunes using runtime cache policy."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    state_store = StateStore(settings.db_path)
    service = GoesTimelapseService(
        settings=settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )

    area = catalog.get("municipio-3205309")
    assert area is not None
    state_store.upsert_tracked(area, status="ready")

    async def fake_build_download_plans():
        return [
            DownloadSourcePlan(
                source_key=RAW_SOURCE_VISIBLE,
                source_label="Visível B2",
                tracked_area_ids=(area.area_id,),
                should_download=True,
                target_timestamps=("20260701830", "20260701820", "20260701810"),
                reason="Teste",
            )
        ]

    async def fake_refresh_latest():
        return DownloadReport(
            kept_files=[],
            downloaded_count=0,
            attempted_count=3,
            failed_count=0,
            last_downloaded=None,
            latest_available=None,
        )

    prune_calls: list[str] = []

    def fake_prune_raw_cache():
        prune_calls.append("called")

    monkeypatch.setattr(service, "_build_download_plans", fake_build_download_plans)
    monkeypatch.setattr(
        service._downloaders[RAW_SOURCE_VISIBLE], "refresh_latest", fake_refresh_latest
    )
    monkeypatch.setattr(service, "_prune_raw_cache", fake_prune_raw_cache)

    await service.refresh_raw_frames()

    assert prune_calls == ["called"]
    state_store.close()


@pytest.mark.asyncio
async def test_refresh_raw_frames_does_not_query_paused_source(settings, monkeypatch):
    """Test refresh raw frames does not query paused source."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    state_store = StateStore(settings.db_path)
    service = GoesTimelapseService(
        settings=settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )

    area = catalog.get("municipio-3205309")
    assert area is not None
    state_store.upsert_tracked(area, status="ready")

    async def fake_build_download_plans(*, reference_moment=None):
        return [
            DownloadSourcePlan(
                source_key=RAW_SOURCE_INFRARED,
                source_label="Infravermelho B13",
                tracked_area_ids=(area.area_id,),
                should_download=False,
                target_timestamps=(),
                reason="Pausado",
            )
        ]

    service._download_status[RAW_SOURCE_INFRARED]["latest_available"] = (
        "existing-latest.tif"
    )
    service._download_status[RAW_SOURCE_INFRARED]["last_downloaded"] = (
        "existing-downloaded.tif"
    )

    async def fail_refresh_latest(*, download_missing=True, target_timestamps=None):
        raise AssertionError("Paused source should not be refreshed remotely")

    monkeypatch.setattr(service, "_build_download_plans", fake_build_download_plans)
    monkeypatch.setattr(
        service._downloaders[RAW_SOURCE_INFRARED], "refresh_latest", fail_refresh_latest
    )

    await service.refresh_raw_frames()

    snapshot = service.downloads_snapshot()
    infrared = next(
        item
        for item in snapshot["sources"]
        if item["source_key"] == RAW_SOURCE_INFRARED
    )
    assert infrared["latest_available"] == "existing-latest.tif"
    assert infrared["last_downloaded"] == "existing-downloaded.tif"
    assert infrared["phase"] == "paused"
    state_store.close()


@pytest.mark.asyncio
async def test_refresh_raw_frames_pauses_when_storage_is_critically_low(
    settings, monkeypatch
):
    """Test refresh raw frames pauses when storage is critically low."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    state_store = StateStore(settings.db_path)
    service = GoesTimelapseService(
        settings=settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )

    area = catalog.get("municipio-3205309")
    assert area is not None
    state_store.upsert_tracked(area, status="ready")

    async def fake_build_download_plans():
        return [
            DownloadSourcePlan(
                source_key=RAW_SOURCE_VISIBLE,
                source_label="Visível B2",
                tracked_area_ids=(area.area_id,),
                should_download=True,
                target_timestamps=("20260701830",),
                reason="Ativo entre o nascer e o pôr do sol dos municípios",
            )
        ]

    async def fail_refresh_latest():
        raise AssertionError("Downloader não deveria rodar com storage crítico")

    monkeypatch.setattr(service, "_build_download_plans", fake_build_download_plans)
    monkeypatch.setattr(
        service._downloaders[RAW_SOURCE_VISIBLE],
        "refresh_latest",
        fail_refresh_latest,
    )
    monkeypatch.setattr(
        service,
        "_storage_checks",
        lambda: {
            "cache": StorageCheck(
                key="cache",
                label="Cache bruto",
                path=settings.data_dir,
                free_bytes=256 * 1024 * 1024,
                total_bytes=100 * 1024 * 1024 * 1024,
                warning=(
                    f"Cache bruto em '{settings.data_dir}' "
                    "sem espaço suficiente (256.0 MB livres)"
                ),
                is_blocking=True,
            ),
            "source": StorageCheck(
                key="source",
                label="Staging de download",
                path=settings.source_dir,
                free_bytes=9 * 1024 * 1024 * 1024,
                total_bytes=20 * 1024 * 1024 * 1024,
                warning=None,
                is_blocking=False,
            ),
            "scratch": StorageCheck(
                key="scratch",
                label="Scratch de conversão",
                path=settings.scratch_dir,
                free_bytes=9 * 1024 * 1024 * 1024,
                total_bytes=20 * 1024 * 1024 * 1024,
                warning=None,
                is_blocking=False,
            ),
        },
    )

    await service.refresh_raw_frames()

    snapshot = service.downloads_snapshot()
    visible = next(
        item for item in snapshot["sources"] if item["source_key"] == RAW_SOURCE_VISIBLE
    )
    assert visible["phase"] == "paused"
    assert "Cache bruto" in str(visible["schedule_reason"])
    assert "Refresh pausado por falta de espaço" in str(
        service._status["last_poll_error"]
    )
    assert service._status["last_poll_finished_at"] is not None
    state_store.close()


@pytest.mark.asyncio
async def test_adding_area_while_refresh_runs_does_not_start_another(
    settings, monkeypatch
):
    """Test adding area while refresh runs does not start another."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    state_store = StateStore(settings.db_path)
    service = GoesTimelapseService(
        settings=settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=True,
    )

    calls: list[str] = []
    release = asyncio.Event()

    async def fake_refresh():
        calls.append("refresh")
        await release.wait()

    monkeypatch.setattr(service, "refresh_raw_frames", fake_refresh)

    await service.add_tracked("municipio-3205309")
    await asyncio.sleep(0)
    await service.add_tracked("municipio-3509502")
    await asyncio.sleep(0)

    assert calls == ["refresh"]
    release.set()
    await service.stop()
    state_store.close()


@pytest.mark.asyncio
async def test_set_marker_validates_and_queues_area(settings, monkeypatch):
    """Test set marker validates and queues area."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    state_store = StateStore(settings.db_path)
    service = GoesTimelapseService(
        settings=settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )
    area = catalog.get("municipio-3205309")
    assert area is not None
    state_store.upsert_tracked(area, status="ready")
    geometry = service.geometry_store.load_geometry(area)

    queued_ids: list[str] = []

    async def fake_enqueue(area_id: str):
        queued_ids.append(area_id)

    monkeypatch.setattr(service, "enqueue", fake_enqueue)

    tracked = await service.set_marker(
        area.area_id,
        marker_lat=geometry.centroid[1],
        marker_lon=geometry.centroid[0],
    )

    assert queued_ids == [area.area_id]
    assert tracked.marker_lat == geometry.centroid[1]
    assert tracked.marker_lon == geometry.centroid[0]
    assert tracked.status == "queued"
    state_store.close()


@pytest.mark.asyncio
async def test_set_marker_accepts_point_in_frame_outside_municipality(
    settings, monkeypatch
):
    """Test set marker accepts point in frame outside municipality."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    state_store = StateStore(settings.db_path)
    service = GoesTimelapseService(
        settings=settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )
    area = catalog.get("municipio-3205309")
    assert area is not None
    state_store.upsert_tracked(area, status="ready")
    geometry = service.geometry_store.load_geometry(area)

    async def fake_enqueue(area_id: str):
        return None

    monkeypatch.setattr(service, "enqueue", fake_enqueue)

    min_lon, _, max_lon, _ = service.renderer.destination_bounds(geometry)
    marker_lat = geometry.centroid[1]

    tracked = await service.set_marker(
        area.area_id, marker_lat=marker_lat, marker_lon=max_lon
    )
    assert tracked.marker_lon == max_lon

    with pytest.raises(ValueError, match="área da imagem"):
        await service.set_marker(
            area.area_id, marker_lat=marker_lat, marker_lon=max_lon + 0.01
        )

    assert geometry.bounds[2] < max_lon
    assert min_lon < geometry.bounds[0]
    state_store.close()


@pytest.mark.asyncio
async def test_clear_marker_queues_area(settings, monkeypatch):
    """Test clear marker queues area."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    state_store = StateStore(settings.db_path)
    service = GoesTimelapseService(
        settings=settings,
        catalog=catalog,
        state_store=state_store,
        start_background_tasks=False,
    )
    area = catalog.get("municipio-3205309")
    assert area is not None
    state_store.upsert_tracked(area, status="ready")
    state_store.set_marker(area.area_id, marker_lat=-20.32, marker_lon=-40.29)

    queued_ids: list[str] = []

    async def fake_enqueue(area_id: str):
        queued_ids.append(area_id)

    monkeypatch.setattr(service, "enqueue", fake_enqueue)

    tracked = await service.clear_marker(area.area_id)

    assert queued_ids == [area.area_id]
    assert tracked.marker_lat is None
    assert tracked.marker_lon is None
    assert tracked.status == "queued"
    state_store.close()
