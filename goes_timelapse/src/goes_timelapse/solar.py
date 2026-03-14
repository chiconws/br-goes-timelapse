from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from astral import Observer
from astral.sun import sun


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
