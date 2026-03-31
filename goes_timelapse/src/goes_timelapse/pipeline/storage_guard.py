from __future__ import annotations

import shutil
from pathlib import Path

from goes_timelapse.core.config import Settings
from goes_timelapse.pipeline.runtime_types import (
    CACHE_BLOCKING_FREE_BYTES,
    CACHE_WARNING_FREE_BYTES,
    STAGING_BLOCKING_FREE_BYTES,
    STAGING_WARNING_FREE_BYTES,
    StorageCheck,
)


def build_storage_checks(settings: Settings) -> dict[str, StorageCheck]:
    return {
        "cache": build_storage_check(
            key="cache",
            label="Cache bruto",
            path=settings.data_dir,
            warning_free_bytes=CACHE_WARNING_FREE_BYTES,
            blocking_free_bytes=CACHE_BLOCKING_FREE_BYTES,
        ),
        "source": build_storage_check(
            key="source",
            label="Staging de download",
            path=settings.source_dir,
            warning_free_bytes=STAGING_WARNING_FREE_BYTES,
            blocking_free_bytes=STAGING_BLOCKING_FREE_BYTES,
        ),
        "scratch": build_storage_check(
            key="scratch",
            label="Scratch de conversão",
            path=settings.scratch_dir,
            warning_free_bytes=STAGING_WARNING_FREE_BYTES,
            blocking_free_bytes=STAGING_BLOCKING_FREE_BYTES,
        ),
    }


def build_storage_check(
    *,
    key: str,
    label: str,
    path: Path,
    warning_free_bytes: int,
    blocking_free_bytes: int,
) -> StorageCheck:
    usage_path = path if path.exists() else path.parent
    usage = shutil.disk_usage(usage_path)
    free_bytes = int(usage.free)
    warning: str | None = None
    is_blocking = free_bytes < blocking_free_bytes

    if is_blocking:
        warning = (
            f"{label} em '{path}' sem espaço suficiente "
            f"({format_bytes(free_bytes)} livres)"
        )
    elif free_bytes < warning_free_bytes:
        warning = (
            f"{label} em '{path}' com pouco espaço "
            f"({format_bytes(free_bytes)} livres)"
        )

    return StorageCheck(
        key=key,
        label=label,
        path=path,
        free_bytes=free_bytes,
        total_bytes=int(usage.total),
        warning=warning,
        is_blocking=is_blocking,
    )


def worst_staging_check(storage_checks: dict[str, StorageCheck]) -> StorageCheck | None:
    staging_checks = [
        storage_checks[key]
        for key in ("source", "scratch")
        if key in storage_checks
    ]
    if not staging_checks:
        return None
    return min(staging_checks, key=lambda check: check.free_bytes)


def storage_warning_summary(storage_checks: dict[str, StorageCheck]) -> str | None:
    warnings: list[str] = []
    for check in storage_checks.values():
        if check.warning and check.warning not in warnings:
            warnings.append(check.warning)
    if not warnings:
        return None
    return " | ".join(warnings)


def blocking_storage_message(storage_checks: dict[str, StorageCheck]) -> str | None:
    blocking_warnings = [
        check.warning
        for check in storage_checks.values()
        if check.is_blocking and check.warning
    ]
    if not blocking_warnings:
        return None
    return "Refresh pausado por falta de espaço: " + " | ".join(blocking_warnings)


def format_bytes(value: int) -> str:
    size = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            decimals = 0 if unit == "B" else 1
            return f"{size:.{decimals}f} {unit}"
        size /= 1024
    return f"{value} B"
