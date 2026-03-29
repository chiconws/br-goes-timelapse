from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from goes_timelapse.solar import (
    DEFAULT_TRANSITION_BLEND_WEIGHTS,
    is_within_visible_window,
    sunrise_transition_alpha,
    sunset_transition_alpha,
)


SOURCE_VISIBLE = "visible"
SOURCE_INFRARED = "infrared"
SOURCE_LIGHTNING = "lightning"

PHASE_VISIBLE = "visible"
PHASE_INFRARED = "infrared"
PHASE_SUNRISE_BLEND = "sunrise_blend"
PHASE_SUNSET_BLEND = "sunset_blend"


@dataclass(slots=True, frozen=True)
class TimelineFrame:
    slot_timestamp: str
    phase: str
    primary_source: str
    blend_source: str | None
    blend_alpha: float | None
    required_sources: tuple[str, ...]


@dataclass(slots=True, frozen=True)
class AreaTimelinePlan:
    area_id: str
    frames: tuple[TimelineFrame, ...]

    @property
    def slot_timestamps(self) -> tuple[str, ...]:
        return tuple(frame.slot_timestamp for frame in self.frames)


def build_area_timeline_plan(
    *,
    area_id: str,
    longitude: float,
    latitude: float,
    frame_count: int,
    end_moment_utc: datetime,
    solar_margin_hours: int,
    slot_minutes: int,
    blend_weights: tuple[float, ...] = DEFAULT_TRANSITION_BLEND_WEIGHTS,
) -> AreaTimelinePlan:
    if frame_count <= 0:
        return AreaTimelinePlan(area_id=area_id, frames=())

    end_slot = floor_to_slot(end_moment_utc, slot_minutes=slot_minutes)
    start_slot = end_slot - timedelta(minutes=slot_minutes * (frame_count - 1))
    frames: list[TimelineFrame] = []

    for index in range(frame_count):
        slot_moment = start_slot + timedelta(minutes=slot_minutes * index)
        slot_timestamp = datetime_to_slot_timestamp(slot_moment)
        sunrise_alpha = sunrise_transition_alpha(
            longitude=longitude,
            latitude=latitude,
            moment_utc=slot_moment,
            blend_weights=blend_weights,
            slot_minutes=slot_minutes,
        )
        sunset_alpha = sunset_transition_alpha(
            longitude=longitude,
            latitude=latitude,
            moment_utc=slot_moment,
            blend_weights=blend_weights,
            slot_minutes=slot_minutes,
        )

        if sunrise_alpha is not None:
            frames.append(
                TimelineFrame(
                    slot_timestamp=slot_timestamp,
                    phase=PHASE_SUNRISE_BLEND,
                    primary_source=SOURCE_INFRARED,
                    blend_source=SOURCE_VISIBLE,
                    blend_alpha=sunrise_alpha,
                    required_sources=(
                        SOURCE_INFRARED,
                        SOURCE_VISIBLE,
                        SOURCE_LIGHTNING,
                    ),
                )
            )
            continue

        if sunset_alpha is not None:
            frames.append(
                TimelineFrame(
                    slot_timestamp=slot_timestamp,
                    phase=PHASE_SUNSET_BLEND,
                    primary_source=SOURCE_VISIBLE,
                    blend_source=SOURCE_INFRARED,
                    blend_alpha=sunset_alpha,
                    required_sources=(
                        SOURCE_VISIBLE,
                        SOURCE_INFRARED,
                        SOURCE_LIGHTNING,
                    ),
                )
            )
            continue

        if is_within_visible_window(
            longitude=longitude,
            latitude=latitude,
            moment_utc=slot_moment,
            margin_hours=solar_margin_hours,
        ).is_open:
            frames.append(
                TimelineFrame(
                    slot_timestamp=slot_timestamp,
                    phase=PHASE_VISIBLE,
                    primary_source=SOURCE_VISIBLE,
                    blend_source=None,
                    blend_alpha=None,
                    required_sources=(SOURCE_VISIBLE, SOURCE_LIGHTNING),
                )
            )
            continue

        frames.append(
            TimelineFrame(
                slot_timestamp=slot_timestamp,
                phase=PHASE_INFRARED,
                primary_source=SOURCE_INFRARED,
                blend_source=None,
                blend_alpha=None,
                required_sources=(SOURCE_INFRARED, SOURCE_LIGHTNING),
            )
        )

    return AreaTimelinePlan(area_id=area_id, frames=tuple(frames))


def floor_to_slot(moment_utc: datetime, *, slot_minutes: int) -> datetime:
    if moment_utc.tzinfo is None:
        moment_utc = moment_utc.replace(tzinfo=UTC)
    else:
        moment_utc = moment_utc.astimezone(UTC)
    floored_minute = moment_utc.minute - (moment_utc.minute % slot_minutes)
    return moment_utc.replace(minute=floored_minute, second=0, microsecond=0)


def datetime_to_slot_timestamp(moment_utc: datetime) -> str:
    if moment_utc.tzinfo is None:
        moment_utc = moment_utc.replace(tzinfo=UTC)
    else:
        moment_utc = moment_utc.astimezone(UTC)
    return moment_utc.strftime("%Y%j%H%M")
