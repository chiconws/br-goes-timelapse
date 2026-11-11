"""Tests for sunrise and sunset windows."""

from __future__ import annotations

from datetime import UTC, date, timedelta

from goes_timelapse.timeline import (
    DEFAULT_TRANSITION_BLEND_WEIGHTS,
    is_within_visible_window,
    sunrise_transition_alpha,
    sunrise_transition_slots,
    sunset_transition_alpha,
    sunset_transition_slots,
    visible_window_for_day,
)


def test_visible_window_for_day_wraps_sunrise_and_sunset():
    """Test visible window for day wraps sunrise and sunset."""
    window = visible_window_for_day(
        longitude=-40.3128,
        latitude=-20.2976,
        day=date(2026, 3, 11),
        margin_hours=2,
    )

    assert window.window_start < window.sunrise < window.sunset < window.window_end


def test_is_within_visible_window_marks_time_inside_margin_as_open():
    """Test is within visible window marks time inside margin as open."""
    window = visible_window_for_day(
        longitude=-40.3128,
        latitude=-20.2976,
        day=date(2026, 3, 11),
        margin_hours=2,
    )
    moment = window.sunrise - timedelta(minutes=30)

    result = is_within_visible_window(
        longitude=-40.3128,
        latitude=-20.2976,
        moment_utc=moment.astimezone(UTC),
        margin_hours=2,
    )

    assert result.is_open is True


def test_sunset_transition_slots_returns_four_utc_slots():
    """Test sunset transition slots returns four utc slots."""
    slots = sunset_transition_slots(
        longitude=-40.3128,
        latitude=-20.2976,
        day=date(2026, 3, 11),
    )

    assert len(slots) == 4
    assert slots[0] < slots[-1]
    assert all(slot.tzinfo is UTC for slot in slots)


def test_sunrise_transition_slots_returns_four_utc_slots():
    """Test sunrise transition slots returns four utc slots."""
    slots = sunrise_transition_slots(
        longitude=-40.3128,
        latitude=-20.2976,
        day=date(2026, 3, 11),
    )

    assert len(slots) == 4
    assert slots[0] < slots[-1]
    assert all(slot.tzinfo is UTC for slot in slots)


def test_sunset_transition_alpha_uses_explicit_weights():
    """Test sunset transition alpha uses explicit weights."""
    slots = sunset_transition_slots(
        longitude=-40.3128,
        latitude=-20.2976,
        day=date(2026, 3, 11),
    )

    alphas = [
        sunset_transition_alpha(
            longitude=-40.3128,
            latitude=-20.2976,
            moment_utc=slot,
        )
        for slot in slots
    ]

    assert alphas == list(DEFAULT_TRANSITION_BLEND_WEIGHTS)


def test_sunrise_transition_alpha_uses_explicit_weights():
    """Test sunrise transition alpha uses explicit weights."""
    slots = sunrise_transition_slots(
        longitude=-40.3128,
        latitude=-20.2976,
        day=date(2026, 3, 11),
    )

    alphas = [
        sunrise_transition_alpha(
            longitude=-40.3128,
            latitude=-20.2976,
            moment_utc=slot,
        )
        for slot in slots
    ]

    assert alphas == list(DEFAULT_TRANSITION_BLEND_WEIGHTS)
