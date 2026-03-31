from __future__ import annotations
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from rasterio import Affine

from goes_timelapse.core.logging_utils import resolve_log_level


REMOTE_FILESYSTEM_TYPES = {
    "nfs",
    "nfs4",
    "cifs",
    "smb",
    "smb2",
    "smb3",
    "sshfs",
    "fuse.sshfs",
    "davfs",
    "glusterfs",
    "ceph",
    "cephfs",
}


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
    state_dir: Path
    work_dir: Path
    scratch_dir: Path
    requested_scratch_dir: Path
    scratch_dir_warning: str | None
    source_dir: Path
    source_dir_warning: str | None
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
        package_dir = Path(__file__).resolve().parent.parent
        data_dir = _env_path("GOES_DATA_DIR", "/data/goes_timelapse")
        state_dir = _env_path("GOES_STATE_DIR", "/config/goes_timelapse/state")
        work_dir = _env_path("GOES_WORK_DIR", "/tmp/goes_timelapse")
        requested_scratch_dir = _env_path(
            "GOES_SCRATCH_DIR",
            str(data_dir / "tmp"),
        )
        source_dir, source_dir_warning = _resolve_source_dir(
            data_dir / "source",
            work_dir / "source",
        )
        scratch_dir, scratch_dir_warning = _resolve_scratch_dir(
            requested_scratch_dir,
            work_dir / "scratch",
        )
        raw_dir = data_dir / "raw"
        processed_dir = data_dir / "processed"
        geometry_cache_dir = data_dir / "geometry"
        media_dir = _env_path("GOES_MEDIA_DIR", "/media/goes_timelapse")
        snippets_dir = _env_path("GOES_SNIPPETS_DIR", "/config/goes_timelapse/lovelace")
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
            state_dir=state_dir,
            work_dir=work_dir,
            scratch_dir=scratch_dir,
            requested_scratch_dir=requested_scratch_dir,
            scratch_dir_warning=scratch_dir_warning,
            source_dir=source_dir,
            source_dir_warning=source_dir_warning,
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
            self.state_dir,
            self.work_dir,
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
    remote_fs_type = _filesystem_type_for_path(requested_scratch_dir)
    if remote_fs_type in REMOTE_FILESYSTEM_TYPES:
        _ensure_writable_dir(fallback_scratch_dir)
        return (
            fallback_scratch_dir,
            (
                f"scratch_dir '{requested_scratch_dir}' está em filesystem remoto "
                f"({remote_fs_type}); usando staging local '{fallback_scratch_dir}'"
            ),
        )
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


def _resolve_source_dir(
    requested_source_dir: Path,
    fallback_source_dir: Path,
) -> tuple[Path, str | None]:
    remote_fs_type = _filesystem_type_for_path(requested_source_dir)
    if remote_fs_type in REMOTE_FILESYSTEM_TYPES:
        _ensure_writable_dir(fallback_source_dir)
        return (
            fallback_source_dir,
            (
                f"source_dir '{requested_source_dir}' está em filesystem remoto "
                f"({remote_fs_type}); usando staging local '{fallback_source_dir}'"
            ),
        )
    try:
        _ensure_writable_dir(requested_source_dir)
        return requested_source_dir, None
    except OSError as err:
        _ensure_writable_dir(fallback_source_dir)
        return (
            fallback_source_dir,
            (
                f"source_dir '{requested_source_dir}' não está gravável; "
                f"usando fallback '{fallback_source_dir}' ({err})"
            ),
        )


def _ensure_writable_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    probe = path / ".write-test"
    probe.write_bytes(b"ok")
    probe.unlink(missing_ok=True)


def _filesystem_type_for_path(path: Path) -> str | None:
    stat_command = shutil.which("stat")
    if stat_command is not None:
        try:
            result = subprocess.run(
                [stat_command, "-f", "-c", "%T", str(path.expanduser())],
                capture_output=True,
                text=True,
                check=False,
                timeout=2,
            )
        except (OSError, subprocess.SubprocessError):
            result = None
        if result is not None and result.returncode == 0:
            detected = result.stdout.strip()
            if detected:
                return detected

    mounts_path = Path("/proc/mounts")
    if not mounts_path.exists():
        return None

    try:
        target = str(path.expanduser().resolve(strict=False))
    except OSError:
        target = str(path.expanduser())

    best_match = ""
    best_fs_type: str | None = None
    for line in mounts_path.read_text(encoding="utf-8").splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        mount_point = parts[1].replace("\\040", " ")
        fs_type = parts[2]
        normalized_mount = mount_point.rstrip("/") or "/"
        if target == normalized_mount or target.startswith(normalized_mount + os.sep):
            if len(normalized_mount) > len(best_match):
                best_match = normalized_mount
                best_fs_type = fs_type
    return best_fs_type
