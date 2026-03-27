from __future__ import annotations

import logging
import os
import shutil
from dataclasses import dataclass
from pathlib import Path

from rasterio import Affine


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
    host: str
    port: int
    goes_url: str
    poll_minutes: int
    frame_count: int
    gif_fps: int
    raw_history: int
    solar_margin_hours: int
    max_tracked: int
    log_level: int
    data_dir: Path
    scratch_dir: Path
    requested_scratch_dir: Path
    scratch_dir_warning: str | None
    source_dir: Path
    raw_dir: Path
    processed_dir: Path
    geometry_cache_dir: Path
    media_dir: Path
    snippets_dir: Path
    db_path: Path
    catalog_path: Path
    state_boundaries_path: Path
    ibge_malhas_url: str
    ibge_request_timeout: int
    font_path: Path
    allowed_client_hosts: tuple[str, ...]
    max_render_dimension: int
    transform: Affine

    @classmethod
    def from_env(cls) -> "Settings":
        package_dir = Path(__file__).resolve().parent
        data_dir = _env_path("GOES_DATA_DIR", "/data/goes_timelapse")
        requested_scratch_dir = _env_path("GOES_SCRATCH_DIR", "/dev/shm/goes_timelapse")
        scratch_dir, scratch_dir_warning = _resolve_scratch_dir(
            requested_scratch_dir,
            data_dir / "tmp",
        )
        source_dir = data_dir / "source"
        raw_dir = data_dir / "raw"
        processed_dir = data_dir / "processed"
        geometry_cache_dir = data_dir / "geometry"
        media_dir = _env_path("GOES_MEDIA_DIR", "/media/goes_timelapse")
        snippets_dir = _env_path("GOES_SNIPPETS_DIR", "/config/goes_timelapse/lovelace")
        db_path = data_dir / "state.db"
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
        log_level_name = os.getenv("GOES_LOG_LEVEL", "INFO").upper()
        log_level = getattr(logging, log_level_name, logging.INFO)
        return cls(
            host=os.getenv("GOES_HOST", "0.0.0.0"),
            port=_env_int("GOES_PORT", 8099),
            goes_url=os.getenv(
                "GOES_URL", "https://noaa-goes19.s3.amazonaws.com/"
            ),
            poll_minutes=_env_int("GOES_POLL_MINUTES", 2),
            frame_count=_env_int("GOES_FRAME_COUNT", 10),
            gif_fps=_env_int("GOES_GIF_FPS", 2),
            raw_history=_env_int("GOES_RAW_HISTORY", 12),
            solar_margin_hours=_env_int("GOES_SOLAR_MARGIN_HOURS", 0),
            max_tracked=_env_int("GOES_MAX_TRACKED", 5),
            log_level=log_level,
            data_dir=data_dir,
            scratch_dir=scratch_dir,
            requested_scratch_dir=requested_scratch_dir,
            scratch_dir_warning=scratch_dir_warning,
            source_dir=source_dir,
            raw_dir=raw_dir,
            processed_dir=processed_dir,
            geometry_cache_dir=geometry_cache_dir,
            media_dir=media_dir,
            snippets_dir=snippets_dir,
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
            transform=Affine(
                0.008997,
                0.0,
                -151.4654998779297,
                0.0,
                -0.008997,
                76.46549987792969,
            ),
        )

    def ensure_directories(self) -> None:
        for directory in (
            self.data_dir,
            self.scratch_dir,
            self.source_dir,
            self.raw_dir,
            self.processed_dir,
            self.geometry_cache_dir,
            self.media_dir,
            self.snippets_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)
        self._cleanup_stale_temp_dirs()

    def configure_runtime_environment(self) -> None:
        scratch_dir = str(self.scratch_dir)
        os.environ["TMPDIR"] = scratch_dir
        os.environ["TMP"] = scratch_dir
        os.environ["TEMP"] = scratch_dir

    def _cleanup_stale_temp_dirs(self) -> None:
        cleanup_roots = (
            self.scratch_dir,
            self.raw_dir / "visible",
            self.raw_dir / "infrared",
        )
        for root in cleanup_roots:
            for path in root.glob("geo2grid-*"):
                if path.is_dir():
                    shutil.rmtree(path, ignore_errors=True)
                else:
                    path.unlink(missing_ok=True)


def _resolve_scratch_dir(
    requested_scratch_dir: Path,
    fallback_scratch_dir: Path,
) -> tuple[Path, str | None]:
    try:
        _ensure_writable_dir(requested_scratch_dir)
        return requested_scratch_dir, None
    except OSError as err:
        _ensure_writable_dir(fallback_scratch_dir)
        return (
            fallback_scratch_dir,
            (
                f"scratch_dir '{requested_scratch_dir}' não está gravável; "
                f"usando fallback '{fallback_scratch_dir}' ({err})"
            ),
        )


def _ensure_writable_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    probe = path / ".write-test"
    probe.write_bytes(b"ok")
    probe.unlink(missing_ok=True)
