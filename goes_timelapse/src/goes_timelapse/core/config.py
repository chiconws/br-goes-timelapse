"""Runtime settings read from the add-on options."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path

from goes_timelapse.core.logging_utils import resolve_log_level


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    return int(value)


def _env_path(name: str, default: str) -> Path:
    value = os.getenv(name)
    if value is None:
        return Path(default).expanduser()
    normalized = value.strip()
    if not normalized or normalized.lower() == "null":
        return Path(default).expanduser()
    return Path(normalized).expanduser()


@dataclass(slots=True, frozen=True)
class Settings:
    """Paths, limits and NOAA endpoints for one run of the app."""

    host: str
    port: int
    goes_url: str
    poll_minutes: int
    frame_count: int
    gif_fps: int
    solar_margin_hours: int
    max_tracked: int
    log_level: int
    data_dir: Path
    state_dir: Path
    raw_dir: Path
    processed_dir: Path
    geometry_cache_dir: Path
    media_dir: Path
    db_path: Path
    catalog_path: Path
    state_boundaries_path: Path
    ibge_malhas_url: str
    ibge_request_timeout: int
    font_path: Path
    allowed_client_hosts: tuple[str, ...]
    max_render_dimension: int

    @classmethod
    def from_env(cls) -> Settings:
        """Build settings from the add-on options file and environment."""
        package_dir = Path(__file__).resolve().parent.parent
        data_dir = _env_path("GOES_DATA_DIR", "/data/goes_timelapse")
        state_dir = _env_path("GOES_STATE_DIR", "/config/goes_timelapse/state")
        raw_dir = data_dir / "raw"
        processed_dir = data_dir / "processed"
        geometry_cache_dir = data_dir / "geometry"
        media_dir = _env_path("GOES_MEDIA_DIR", "/media/goes_timelapse")
        db_path = state_dir / "state.db"
        catalog_path = _env_path(
            "GOES_CATALOG_PATH",
            str(package_dir / "assets" / "areas.json.gz"),
        )
        state_boundaries_path = _env_path(
            "GOES_STATE_BOUNDARIES_PATH",
            str(package_dir / "assets" / "state_boundaries.json.gz"),
        )
        allowed_hosts = tuple(
            part.strip()
            for part in os.getenv(
                "GOES_ALLOWED_CLIENT_HOSTS",
                "127.0.0.1,::1,testclient,172.30.32.2",
            ).split(",")
            if part.strip()
        )
        log_level = resolve_log_level(os.getenv("GOES_LOG_LEVEL", "INFO"))
        return cls(
            host=os.getenv("GOES_HOST", "0.0.0.0"),
            port=_env_int("GOES_PORT", 8099),
            goes_url=os.getenv("GOES_URL", "https://noaa-goes19.s3.amazonaws.com/"),
            poll_minutes=_env_int("GOES_POLL_MINUTES", 2),
            frame_count=_env_int("GOES_FRAME_COUNT", 10),
            gif_fps=_env_int("GOES_GIF_FPS", 2),
            solar_margin_hours=_env_int("GOES_SOLAR_MARGIN_HOURS", 0),
            max_tracked=_env_int("GOES_MAX_TRACKED", 5),
            log_level=log_level,
            data_dir=data_dir,
            state_dir=state_dir,
            raw_dir=raw_dir,
            processed_dir=processed_dir,
            geometry_cache_dir=geometry_cache_dir,
            media_dir=media_dir,
            db_path=db_path,
            catalog_path=catalog_path,
            state_boundaries_path=state_boundaries_path,
            ibge_malhas_url=os.getenv(
                "GOES_IBGE_MALHAS_URL",
                "https://servicodados.ibge.gov.br/api/v4/malhas",
            ),
            ibge_request_timeout=_env_int("GOES_IBGE_TIMEOUT_SECONDS", 30),
            font_path=_env_path(
                "GOES_FONT_PATH",
                "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            ),
            allowed_client_hosts=allowed_hosts,
            max_render_dimension=_env_int("GOES_MAX_RENDER_DIMENSION", 900),
        )

    def ensure_directories(self) -> None:
        """Create every data, state and output directory."""
        for directory in (
            self.data_dir,
            self.state_dir,
            self.raw_dir,
            self.processed_dir,
            self.geometry_cache_dir,
            self.media_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)
