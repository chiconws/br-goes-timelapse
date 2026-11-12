"""Tests for settings from the add-on options."""

from __future__ import annotations

from goes_timelapse.core import config as config_module
from goes_timelapse.core.config import Settings


def test_settings_from_env_uses_two_minute_poll_default(monkeypatch):
    """Test settings from env uses two minute poll default."""
    monkeypatch.delenv("GOES_POLL_MINUTES", raising=False)

    settings = Settings.from_env()

    assert settings.poll_minutes == 2


def test_settings_from_env_uses_default_scratch_dir(monkeypatch, tmp_path):
    """Test settings from env uses default scratch dir."""
    monkeypatch.setenv("GOES_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.delenv("GOES_SCRATCH_DIR", raising=False)
    monkeypatch.delenv("GOES_WORK_DIR", raising=False)

    settings = Settings.from_env()

    assert settings.scratch_dir == settings.requested_scratch_dir
    assert settings.scratch_dir == settings.data_dir / "tmp"


def test_settings_from_env_uses_separate_state_dir_default(monkeypatch):
    """Test settings from env uses separate state dir default."""
    monkeypatch.delenv("GOES_STATE_DIR", raising=False)

    settings = Settings.from_env()

    assert settings.state_dir.parts[-2:] == ("goes_timelapse", "state")
    assert settings.db_path == settings.state_dir / "state.db"
    assert settings.db_path.parent != settings.data_dir


def test_settings_from_env_treats_null_scratch_dir_as_unset(monkeypatch, tmp_path):
    """Test settings from env treats null scratch dir as unset."""
    monkeypatch.setenv("GOES_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("GOES_SCRATCH_DIR", "null")

    settings = Settings.from_env()

    assert settings.scratch_dir == settings.data_dir / "tmp"


def test_settings_from_env_falls_back_to_local_work_scratch_when_scratch_dir_is_not_writable(
    monkeypatch,
    tmp_path,
):
    """Test settings from env falls back to local work scratch when scratch dir is not writable."""
    data_dir = tmp_path / "data"
    work_dir = tmp_path / "work"
    requested_scratch_dir = tmp_path / "scratch"

    monkeypatch.setenv("GOES_DATA_DIR", str(data_dir))
    monkeypatch.setenv("GOES_WORK_DIR", str(work_dir))
    monkeypatch.setenv("GOES_SCRATCH_DIR", str(requested_scratch_dir))

    def fake_ensure_writable_dir(path):
        if path == requested_scratch_dir:
            raise OSError("read-only")
        path.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(config_module, "_ensure_writable_dir", fake_ensure_writable_dir)

    settings = Settings.from_env()

    assert settings.requested_scratch_dir == requested_scratch_dir
    assert settings.scratch_dir == work_dir / "scratch"
    assert settings.scratch_dir_warning is not None


def test_settings_from_env_uses_local_staging_for_remote_filesystems(
    monkeypatch, tmp_path
):
    """Test settings from env uses local staging for remote filesystems."""
    data_dir = tmp_path / "data"
    work_dir = tmp_path / "work"
    requested_scratch_dir = data_dir / "tmp"

    monkeypatch.setenv("GOES_DATA_DIR", str(data_dir))
    monkeypatch.setenv("GOES_WORK_DIR", str(work_dir))

    monkeypatch.setattr(
        config_module,
        "_filesystem_type_for_path",
        lambda path: (
            "nfs4" if path in {data_dir / "source", requested_scratch_dir} else None
        ),
    )

    settings = Settings.from_env()

    assert settings.source_dir == work_dir / "source"
    assert settings.scratch_dir == work_dir / "scratch"
    assert settings.source_dir_warning is not None
    assert settings.scratch_dir_warning is not None


def test_ensure_directories_cleans_legacy_geo2grid_dirs(tmp_path):
    """Test ensure directories cleans legacy geo2grid dirs."""
    settings = Settings(
        host="127.0.0.1",
        port=8099,
        goes_url="https://example.invalid/",
        poll_minutes=2,
        frame_count=10,
        gif_fps=2,
        raw_history=12,
        solar_margin_hours=0,
        max_tracked=5,
        log_level=20,
        data_dir=tmp_path / "data",
        state_dir=tmp_path / "state",
        work_dir=tmp_path / "work",
        scratch_dir=tmp_path / "scratch",
        requested_scratch_dir=tmp_path / "scratch",
        scratch_dir_warning=None,
        source_dir=tmp_path / "work" / "source",
        source_dir_warning=None,
        raw_dir=tmp_path / "data" / "raw",
        processed_dir=tmp_path / "data" / "processed",
        geometry_cache_dir=tmp_path / "data" / "geometry",
        media_dir=tmp_path / "media",
        snippets_dir=tmp_path / "config" / "goes_timelapse" / "lovelace",
        db_path=tmp_path / "state" / "state.db",
        catalog_path=tmp_path / "areas.json.gz",
        state_boundaries_path=tmp_path / "states.json.gz",
        ibge_malhas_url="https://example.invalid/api/v4/malhas",
        ibge_request_timeout=5,
        font_path=tmp_path / "font.ttf",
        allowed_client_hosts=("testclient",),
        max_render_dimension=900,
    )

    legacy_dirs = [
        settings.scratch_dir / "geo2grid-new",
        settings.raw_dir / "visible" / "geo2grid-old-visible",
        settings.raw_dir / "infrared" / "geo2grid-old-infrared",
    ]
    for path in legacy_dirs:
        path.mkdir(parents=True, exist_ok=True)
        (path / "marker.txt").write_text("stale", encoding="utf-8")

    settings.ensure_directories()

    for path in legacy_dirs:
        assert not path.exists()
