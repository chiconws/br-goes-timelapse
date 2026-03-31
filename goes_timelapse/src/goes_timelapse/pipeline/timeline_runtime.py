from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Callable

from goes_timelapse.core.config import Settings
from goes_timelapse.core.logging_utils import get_logger
from goes_timelapse.core.models import AreaCatalogEntry
from goes_timelapse.pipeline.download_status import pluralize_slot
from goes_timelapse.pipeline.rendering import FrameSpec
from goes_timelapse.pipeline.runtime_types import (
    RAW_SOURCE_INFRARED,
    RAW_SOURCE_LIGHTNING,
    RAW_SOURCE_VISIBLE,
    SLOT_MINUTES,
)
from goes_timelapse.timeline import (
    AreaTimelinePlan,
    DEFAULT_TRANSITION_BLEND_WEIGHTS,
    build_area_timeline_plan,
    floor_to_slot,
)


TRANSITION_BLEND_WEIGHTS = DEFAULT_TRANSITION_BLEND_WEIGHTS
LOGGER = get_logger(__name__)


def timeline_reference_moment() -> datetime:
    return floor_to_slot(
        datetime.now(UTC) - timedelta(minutes=SLOT_MINUTES),
        slot_minutes=SLOT_MINUTES,
    )


def build_area_timeline_plan_for_area(
    *,
    area: AreaCatalogEntry,
    settings: Settings,
    resolve_area_centroid: Callable[[AreaCatalogEntry], tuple[float, float]],
    reference_moment: datetime | None = None,
) -> AreaTimelinePlan:
    reference_moment = reference_moment or timeline_reference_moment()
    centroid = resolve_area_centroid(area)
    plan = build_area_timeline_plan(
        area_id=area.area_id,
        longitude=centroid[0],
        latitude=centroid[1],
        frame_count=settings.frame_count,
        end_moment_utc=reference_moment,
        solar_margin_hours=settings.solar_margin_hours,
        slot_minutes=SLOT_MINUTES,
        blend_weights=TRANSITION_BLEND_WEIGHTS,
    )
    LOGGER.trace(
        "Timeline plan for %s built at %s with %s frame(s): %s",
        area.area_id,
        reference_moment.isoformat(),
        len(plan.frames),
        [frame.slot_timestamp for frame in plan.frames],
    )
    return plan


def build_global_target_timestamps(
    *,
    tracked_areas: list[AreaCatalogEntry],
    build_area_timeline: Callable[[AreaCatalogEntry, datetime | None], AreaTimelinePlan],
    reference_moment: datetime | None = None,
) -> dict[str, tuple[str, ...]]:
    targets: dict[str, set[str]] = {
        RAW_SOURCE_VISIBLE: set(),
        RAW_SOURCE_INFRARED: set(),
        RAW_SOURCE_LIGHTNING: set(),
    }
    reference_moment = reference_moment or timeline_reference_moment()

    for area in tracked_areas:
        plan = build_area_timeline(area, reference_moment)
        LOGGER.trace(
            "Timeline contributes for %s: %s",
            area.area_id,
            [
                {
                    "slot": frame.slot_timestamp,
                    "required_sources": frame.required_sources,
                    "phase": frame.phase,
                }
                for frame in plan.frames
            ],
        )
        for frame in plan.frames:
            for source_key in frame.required_sources:
                targets[source_key].add(frame.slot_timestamp)

    resolved = {
        source_key: tuple(sorted(values, reverse=True))
        for source_key, values in targets.items()
    }
    LOGGER.trace("Global timeline target timestamps: %s", resolved)
    return resolved


def build_download_plan_reason(
    *,
    source_key: str,
    tracked_area_ids: tuple[str, ...],
    target_timestamps: tuple[str, ...],
) -> str:
    if not tracked_area_ids:
        return "Nenhum município acompanhado"
    if not target_timestamps:
        return {
            RAW_SOURCE_VISIBLE: "Sem slots úteis de B2 na timeline atual",
            RAW_SOURCE_INFRARED: "Sem slots úteis de B13 na timeline atual",
            RAW_SOURCE_LIGHTNING: "Sem slots úteis de descargas na timeline atual",
        }[source_key]

    slot_count = len(target_timestamps)
    short_label = source_short_label(source_key)
    return f"Ativo em {slot_count} {pluralize_slot(slot_count)} úteis de {short_label} na timeline atual"


def source_short_label(source_key: str) -> str:
    return {
        RAW_SOURCE_VISIBLE: "B2",
        RAW_SOURCE_INFRARED: "B13",
        RAW_SOURCE_LIGHTNING: "GLM",
    }[source_key]


def build_frame_specs_from_timeline(
    *,
    area: AreaCatalogEntry,
    build_area_timeline: Callable[[AreaCatalogEntry], AreaTimelinePlan],
    source_paths: dict[str, dict[str, Path]],
    lightning_points_by_timestamp: dict[str, tuple[tuple[float, float], ...]],
) -> list[FrameSpec]:
    timeline_plan = build_area_timeline(area)
    frame_specs: list[FrameSpec] = []
    imagery_paths = {
        RAW_SOURCE_VISIBLE: source_paths[RAW_SOURCE_VISIBLE],
        RAW_SOURCE_INFRARED: source_paths[RAW_SOURCE_INFRARED],
    }

    skipped_frames: list[dict[str, object]] = []
    for frame in timeline_plan.frames:
        primary_path = imagery_paths[frame.primary_source].get(frame.slot_timestamp)
        if primary_path is None:
            skipped_frames.append(
                {
                    "slot": frame.slot_timestamp,
                    "reason": f"missing_primary:{frame.primary_source}",
                }
            )
            continue

        blend_path = None
        blend_alpha = 0.0
        if frame.blend_source is not None:
            blend_path = imagery_paths[frame.blend_source].get(frame.slot_timestamp)
            if blend_path is None or frame.blend_alpha is None:
                skipped_frames.append(
                    {
                        "slot": frame.slot_timestamp,
                        "reason": f"missing_blend:{frame.blend_source}",
                    }
                )
                continue
            blend_alpha = frame.blend_alpha

        frame_specs.append(
            FrameSpec(
                timestamp=frame.slot_timestamp,
                primary_path=primary_path,
                blend_path=blend_path,
                blend_alpha=blend_alpha,
                lightning_points=lightning_points_by_timestamp.get(
                    frame.slot_timestamp,
                    (),
                ),
            )
        )

    LOGGER.trace(
        "Frame specs for %s resolved from timeline: kept=%s skipped=%s",
        area.area_id,
        [frame.timestamp for frame in frame_specs],
        skipped_frames,
    )
    return frame_specs
