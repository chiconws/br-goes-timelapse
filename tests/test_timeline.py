"""Tests for the frame timeline."""

from __future__ import annotations

from datetime import date, timedelta

from goes_timelapse.timeline import (
    DEFAULT_TRANSITION_BLEND_WEIGHTS,
    PHASE_INFRARED,
    PHASE_SUNRISE_BLEND,
    PHASE_SUNSET_BLEND,
    PHASE_VISIBLE,
    SOURCE_INFRARED,
    SOURCE_LIGHTNING,
    SOURCE_VISIBLE,
    build_area_timeline_plan,
    sunrise_transition_slots,
    sunset_transition_slots,
    visible_window_for_day,
)

LONGITUDE = -40.3128
LATITUDE = -20.2976
DAY = date(2026, 3, 11)


def test_build_area_timeline_plan_generates_visible_slots_during_day():
    """Test build area timeline plan generates visible slots during day."""
    window = visible_window_for_day(
        longitude=LONGITUDE,
        latitude=LATITUDE,
        day=DAY,
        margin_hours=0,
    )
    midday = window.sunrise + ((window.sunset - window.sunrise) / 2)

    plan = build_area_timeline_plan(
        area_id="municipio-3205309",
        longitude=LONGITUDE,
        latitude=LATITUDE,
        frame_count=3,
        end_moment_utc=midday,
        solar_margin_hours=0,
    )

    assert len(plan.frames) == 3
    assert all(frame.phase == PHASE_VISIBLE for frame in plan.frames)
    assert all(frame.primary_source == SOURCE_VISIBLE for frame in plan.frames)
    assert all(
        frame.required_sources == (SOURCE_VISIBLE, SOURCE_LIGHTNING)
        for frame in plan.frames
    )


def test_build_area_timeline_plan_generates_infrared_slots_during_night():
    """Test build area timeline plan generates infrared slots during night."""
    window = visible_window_for_day(
        longitude=LONGITUDE,
        latitude=LATITUDE,
        day=DAY,
        margin_hours=0,
    )
    predawn = window.sunrise - timedelta(hours=2)

    plan = build_area_timeline_plan(
        area_id="municipio-3205309",
        longitude=LONGITUDE,
        latitude=LATITUDE,
        frame_count=3,
        end_moment_utc=predawn,
        solar_margin_hours=0,
    )

    assert len(plan.frames) == 3
    assert all(frame.phase == PHASE_INFRARED for frame in plan.frames)
    assert all(frame.primary_source == SOURCE_INFRARED for frame in plan.frames)
    assert all(
        frame.required_sources == (SOURCE_INFRARED, SOURCE_LIGHTNING)
        for frame in plan.frames
    )


def test_build_area_timeline_plan_generates_sunrise_blend_slots():
    """Test build area timeline plan generates sunrise blend slots."""
    slots = sunrise_transition_slots(
        longitude=LONGITUDE,
        latitude=LATITUDE,
        day=DAY,
    )

    plan = build_area_timeline_plan(
        area_id="municipio-3205309",
        longitude=LONGITUDE,
        latitude=LATITUDE,
        frame_count=4,
        end_moment_utc=slots[-1],
        solar_margin_hours=0,
    )

    assert [frame.phase for frame in plan.frames] == [PHASE_SUNRISE_BLEND] * 4
    assert [frame.primary_source for frame in plan.frames] == [SOURCE_INFRARED] * 4
    assert [frame.blend_source for frame in plan.frames] == [SOURCE_VISIBLE] * 4
    assert [frame.blend_alpha for frame in plan.frames] == list(
        DEFAULT_TRANSITION_BLEND_WEIGHTS
    )


def test_build_area_timeline_plan_generates_sunset_blend_slots():
    """Test build area timeline plan generates sunset blend slots."""
    slots = sunset_transition_slots(
        longitude=LONGITUDE,
        latitude=LATITUDE,
        day=DAY,
    )

    plan = build_area_timeline_plan(
        area_id="municipio-3205309",
        longitude=LONGITUDE,
        latitude=LATITUDE,
        frame_count=4,
        end_moment_utc=slots[-1],
        solar_margin_hours=0,
    )

    assert [frame.phase for frame in plan.frames] == [PHASE_SUNSET_BLEND] * 4
    assert [frame.primary_source for frame in plan.frames] == [SOURCE_VISIBLE] * 4
    assert [frame.blend_source for frame in plan.frames] == [SOURCE_INFRARED] * 4
    assert [frame.blend_alpha for frame in plan.frames] == list(
        DEFAULT_TRANSITION_BLEND_WEIGHTS
    )
