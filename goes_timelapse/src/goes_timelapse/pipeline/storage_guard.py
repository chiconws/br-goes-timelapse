"""Free-space check that pauses downloads before the cache disk fills."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil

from goes_timelapse.core.config import Settings

WARNING_FREE_BYTES = 20 * 1024 * 1024 * 1024
BLOCKING_FREE_BYTES = 2 * 1024 * 1024 * 1024


@dataclass(slots=True, frozen=True)
class StorageCheck:
    """Free space on the cache path."""

    path: Path
    free_bytes: int
    total_bytes: int
    warning: str | None
    is_blocking: bool

    @property
    def blocking_message(self) -> str | None:
        """Return why refresh is paused, if it is."""
        if not self.is_blocking:
            return None
        return f"Refresh pausado por falta de espaço: {self.warning}"


def cache_storage_check(settings: Settings) -> StorageCheck:
    """Check free space on the raw cache."""
    path = settings.data_dir
    usage = shutil.disk_usage(path if path.exists() else path.parent)
    free_bytes = int(usage.free)
    is_blocking = free_bytes < BLOCKING_FREE_BYTES
    warning: str | None = None
    if is_blocking:
        warning = (
            f"Cache bruto em '{path}' sem espaço suficiente "
            f"({format_bytes(free_bytes)} livres)"
        )
    elif free_bytes < WARNING_FREE_BYTES:
        warning = f"Cache bruto em '{path}' com pouco espaço ({format_bytes(free_bytes)} livres)"
    return StorageCheck(
        path=path,
        free_bytes=free_bytes,
        total_bytes=int(usage.total),
        warning=warning,
        is_blocking=is_blocking,
    )


def format_bytes(value: int) -> str:
    """Format a byte count for the UI."""
    size = float(value)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.{0 if unit == 'B' else 1}f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"
