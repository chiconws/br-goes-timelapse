from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from astral import Observer
from astral.sun import sun

DEFAULT_TRANSITION_BLEND_WEIGHTS = (0.2, 0.4, 0.6, 0.8)


@dataclass(slots=True, frozen=True)
class SolarWindow:
    is_open: bool
    window_start: datetime
    window_end: datetime
    sunrise: datetime
    sunset: datetime


def visible_window_for_day(
    *,
    longitude: float,
    latitude: float,
    day: date,
    margin_hours: int,
) -> SolarWindow:
    observer = Observer(latitude=latitude, longitude=longitude)
    solar_times = sun(observer, date=day, tzinfo=UTC)
    margin = timedelta(hours=margin_hours)
    sunrise = solar_times["sunrise"]
    sunset = solar_times["sunset"]
    return SolarWindow(
        is_open=False,
        window_start=sunrise - margin,
        window_end=sunset + margin,
        sunrise=sunrise,
        sunset=sunset,
    )


def is_within_visible_window(
    *,
    longitude: float,
    latitude: float,
    moment_utc: datetime,
    margin_hours: int,
) -> SolarWindow:
    if moment_utc.tzinfo is None:
        moment_utc = moment_utc.replace(tzinfo=UTC)
    else:
        moment_utc = moment_utc.astimezone(UTC)

    window = visible_window_for_day(
        longitude=longitude,
        latitude=latitude,
        day=moment_utc.date(),
        margin_hours=margin_hours,
    )
    is_open = window.window_start <= moment_utc <= window.window_end
    return SolarWindow(
        is_open=is_open,
        window_start=window.window_start,
        window_end=window.window_end,
        sunrise=window.sunrise,
        sunset=window.sunset,
    )


def sunset_transition_slots(
    *,
    longitude: float,
    latitude: float,
    day: date,
    frame_count: int = len(DEFAULT_TRANSITION_BLEND_WEIGHTS),
    slot_minutes: int = 10,
) -> tuple[datetime, ...]:
    window = visible_window_for_day(
        longitude=longitude,
        latitude=latitude,
        day=day,
        margin_hours=0,
    )
    sunset_slot = _floor_to_slot(window.sunset, slot_minutes)
    return tuple(
        sunset_slot - timedelta(minutes=slot_minutes * offset)
        for offset in range(frame_count - 1, -1, -1)
    )


def sunrise_transition_slots(
    *,
    longitude: float,
    latitude: float,
    day: date,
    frame_count: int = len(DEFAULT_TRANSITION_BLEND_WEIGHTS),
    slot_minutes: int = 10,
) -> tuple[datetime, ...]:
    window = visible_window_for_day(
        longitude=longitude,
        latitude=latitude,
        day=day,
        margin_hours=0,
    )
    sunrise_slot = _floor_to_slot(window.sunrise, slot_minutes)
    return tuple(
        sunrise_slot + timedelta(minutes=slot_minutes * offset)
        for offset in range(frame_count)
    )


def sunset_transition_alpha(
    *,
    longitude: float,
    latitude: float,
    moment_utc: datetime,
    blend_weights: tuple[float, ...] = DEFAULT_TRANSITION_BLEND_WEIGHTS,
    slot_minutes: int = 10,
) -> float | None:
    if moment_utc.tzinfo is None:
        moment_utc = moment_utc.replace(tzinfo=UTC)
    else:
        moment_utc = moment_utc.astimezone(UTC)

    return _transition_alpha(
        slots=sunset_transition_slots(
            longitude=longitude,
            latitude=latitude,
            day=moment_utc.date(),
            frame_count=len(blend_weights),
            slot_minutes=slot_minutes,
        ),
        blend_weights=blend_weights,
        moment_utc=moment_utc,
        slot_minutes=slot_minutes,
    )


def sunrise_transition_alpha(
    *,
    longitude: float,
    latitude: float,
    moment_utc: datetime,
    blend_weights: tuple[float, ...] = DEFAULT_TRANSITION_BLEND_WEIGHTS,
    slot_minutes: int = 10,
) -> float | None:
    if moment_utc.tzinfo is None:
        moment_utc = moment_utc.replace(tzinfo=UTC)
    else:
        moment_utc = moment_utc.astimezone(UTC)

    return _transition_alpha(
        slots=sunrise_transition_slots(
            longitude=longitude,
            latitude=latitude,
            day=moment_utc.date(),
            frame_count=len(blend_weights),
            slot_minutes=slot_minutes,
        ),
        blend_weights=blend_weights,
        moment_utc=moment_utc,
        slot_minutes=slot_minutes,
    )


def _transition_alpha(
    *,
    slots: tuple[datetime, ...],
    blend_weights: tuple[float, ...],
    moment_utc: datetime,
    slot_minutes: int,
) -> float | None:
    moment_slot = _floor_to_slot(moment_utc, slot_minutes)
    try:
        index = slots.index(moment_slot)
    except ValueError:
        return None
    return blend_weights[index]


def _floor_to_slot(moment_utc: datetime, slot_minutes: int) -> datetime:
    floored_minute = moment_utc.minute - (moment_utc.minute % slot_minutes)
    return moment_utc.replace(minute=floored_minute, second=0, microsecond=0)
