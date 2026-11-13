"""Tests for settings from the add-on options."""

from __future__ import annotations

from goes_timelapse.core.config import Settings


def test_settings_from_env_uses_two_minute_poll_default(monkeypatch):
    """Test settings from env uses two minute poll default."""
    monkeypatch.delenv("GOES_POLL_MINUTES", raising=False)

    settings = Settings.from_env()

    assert settings.poll_minutes == 2


def test_settings_from_env_uses_separate_state_dir_default(monkeypatch):
    """Test settings from env uses separate state dir default."""
    monkeypatch.delenv("GOES_STATE_DIR", raising=False)

    settings = Settings.from_env()

    assert settings.state_dir.parts[-2:] == ("goes_timelapse", "state")
    assert settings.db_path == settings.state_dir / "state.db"
    assert settings.db_path.parent != settings.data_dir
