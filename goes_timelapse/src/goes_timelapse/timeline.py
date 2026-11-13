"""Frame timeline: which slots to show and how to blend day and night."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from astral import Observer
from astral.sun import sun

SLOT_MINUTES = 10
DEFAULT_TRANSITION_BLEND_WEIGHTS = (0.2, 0.4, 0.6, 0.8)

SOURCE_VISIBLE = "visible"
SOURCE_INFRARED = "infrared"
SOURCE_LIGHTNING = "lightning"

PHASE_VISIBLE = "visible"
PHASE_INFRARED = "infrared"
PHASE_SUNRISE_BLEND = "sunrise_blend"
PHASE_SUNSET_BLEND = "sunset_blend"


@dataclass(slots=True, frozen=True)
class SolarWindow:
    """Visible-light window of one day at one place."""

    window_start: datetime
    window_end: datetime
    sunrise: datetime
    sunset: datetime


@dataclass(slots=True, frozen=True)
class TimelineFrame:
    """One frame of the timeline and the sources it uses."""

    slot_timestamp: str
    phase: str
    primary_source: str
    blend_source: str | None
    blend_alpha: float | None
    required_sources: tuple[str, ...]


@dataclass(slots=True, frozen=True)
class AreaTimelinePlan:
    """Ordered frames of one municipality."""

    area_id: str
    frames: tuple[TimelineFrame, ...]


def floor_to_slot(
    moment_utc: datetime, *, slot_minutes: int = SLOT_MINUTES
) -> datetime:
    """Round a time down to the slot grid, in UTC."""
    moment_utc = _as_utc(moment_utc)
    floored_minute = moment_utc.minute - (moment_utc.minute % slot_minutes)
    return moment_utc.replace(minute=floored_minute, second=0, microsecond=0)


def datetime_to_slot_timestamp(moment_utc: datetime) -> str:
    """Format a time as a GOES slot timestamp, in UTC."""
    return _as_utc(moment_utc).strftime("%Y%j%H%M")


def visible_window_for_day(
    *,
    longitude: float,
    latitude: float,
    day: date,
    margin_hours: int,
) -> SolarWindow:
    """Return the visible window for a day at a location."""
    solar_times = sun(
        Observer(latitude=latitude, longitude=longitude), date=day, tzinfo=UTC
    )
    margin = timedelta(hours=margin_hours)
    return SolarWindow(
        window_start=solar_times["sunrise"] - margin,
        window_end=solar_times["sunset"] + margin,
        sunrise=solar_times["sunrise"],
        sunset=solar_times["sunset"],
    )


def is_within_visible_window(
    *,
    longitude: float,
    latitude: float,
    moment_utc: datetime,
    margin_hours: int,
) -> bool:
    """Return whether a moment falls in the visible window."""
    moment_utc = _as_utc(moment_utc)
    window = visible_window_for_day(
        longitude=longitude,
        latitude=latitude,
        day=moment_utc.date(),
        margin_hours=margin_hours,
    )
    return window.window_start <= moment_utc <= window.window_end


def sunrise_transition_slots(
    *, longitude: float, latitude: float, day: date
) -> tuple[datetime, ...]:
    """Return the slots of the sunrise crossfade, starting at sunrise."""
    window = visible_window_for_day(
        longitude=longitude, latitude=latitude, day=day, margin_hours=0
    )
    first = floor_to_slot(window.sunrise)
    return tuple(
        first + timedelta(minutes=SLOT_MINUTES * offset)
        for offset in range(len(DEFAULT_TRANSITION_BLEND_WEIGHTS))
    )


def sunset_transition_slots(
    *, longitude: float, latitude: float, day: date
) -> tuple[datetime, ...]:
    """Return the slots of the sunset crossfade, ending at sunset."""
    window = visible_window_for_day(
        longitude=longitude, latitude=latitude, day=day, margin_hours=0
    )
    last = floor_to_slot(window.sunset)
    return tuple(
        last - timedelta(minutes=SLOT_MINUTES * offset)
        for offset in reversed(range(len(DEFAULT_TRANSITION_BLEND_WEIGHTS)))
    )


def sunrise_transition_alpha(
    *, longitude: float, latitude: float, moment_utc: datetime
) -> float | None:
    """Return the crossfade weight at a sunrise slot, or None outside it."""
    moment_utc = _as_utc(moment_utc)
    return _transition_alpha(
        sunrise_transition_slots(
            longitude=longitude, latitude=latitude, day=moment_utc.date()
        ),
        moment_utc,
    )


def sunset_transition_alpha(
    *, longitude: float, latitude: float, moment_utc: datetime
) -> float | None:
    """Return the crossfade weight at a sunset slot, or None outside it."""
    moment_utc = _as_utc(moment_utc)
    return _transition_alpha(
        sunset_transition_slots(
            longitude=longitude, latitude=latitude, day=moment_utc.date()
        ),
        moment_utc,
    )


def build_area_timeline_plan(
    *,
    area_id: str,
    longitude: float,
    latitude: float,
    frame_count: int,
    end_moment_utc: datetime,
    solar_margin_hours: int,
) -> AreaTimelinePlan:
    """Choose the frames for a municipality around now."""
    end_slot = floor_to_slot(end_moment_utc)
    frames: list[TimelineFrame] = []
    for index in range(frame_count):
        slot_moment = end_slot - timedelta(
            minutes=SLOT_MINUTES * (frame_count - 1 - index)
        )
        slot_timestamp = datetime_to_slot_timestamp(slot_moment)
        if (
            alpha := sunrise_transition_alpha(
                longitude=longitude, latitude=latitude, moment_utc=slot_moment
            )
        ) is not None:
            frame = TimelineFrame(
                slot_timestamp,
                PHASE_SUNRISE_BLEND,
                SOURCE_INFRARED,
                SOURCE_VISIBLE,
                alpha,
                (SOURCE_INFRARED, SOURCE_VISIBLE, SOURCE_LIGHTNING),
            )
        elif (
            alpha := sunset_transition_alpha(
                longitude=longitude, latitude=latitude, moment_utc=slot_moment
            )
        ) is not None:
            frame = TimelineFrame(
                slot_timestamp,
                PHASE_SUNSET_BLEND,
                SOURCE_VISIBLE,
                SOURCE_INFRARED,
                alpha,
                (SOURCE_VISIBLE, SOURCE_INFRARED, SOURCE_LIGHTNING),
            )
        elif is_within_visible_window(
            longitude=longitude,
            latitude=latitude,
            moment_utc=slot_moment,
            margin_hours=solar_margin_hours,
        ):
            frame = TimelineFrame(
                slot_timestamp,
                PHASE_VISIBLE,
                SOURCE_VISIBLE,
                None,
                None,
                (SOURCE_VISIBLE, SOURCE_LIGHTNING),
            )
        else:
            frame = TimelineFrame(
                slot_timestamp,
                PHASE_INFRARED,
                SOURCE_INFRARED,
                None,
                None,
                (SOURCE_INFRARED, SOURCE_LIGHTNING),
            )
        frames.append(frame)
    return AreaTimelinePlan(area_id=area_id, frames=tuple(frames))


def _transition_alpha(
    slots: tuple[datetime, ...], moment_utc: datetime
) -> float | None:
    try:
        return DEFAULT_TRANSITION_BLEND_WEIGHTS[slots.index(floor_to_slot(moment_utc))]
    except ValueError:
        return None


def _as_utc(moment: datetime) -> datetime:
    return (
        moment.replace(tzinfo=UTC) if moment.tzinfo is None else moment.astimezone(UTC)
    )
