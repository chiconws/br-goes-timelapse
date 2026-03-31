from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Callable

from goes_timelapse.core.config import Settings
from goes_timelapse.core.logging_utils import TraceLogger, get_logger
from goes_timelapse.pipeline.downloader import GlmDownloader, GoesDownloader
from goes_timelapse.pipeline.rendering import parse_goes_timestamp
from goes_timelapse.pipeline.runtime_types import (
    RawFileEntry,
    RAW_SOURCE_INFRARED,
    RAW_SOURCE_LABELS,
    RAW_SOURCE_LIGHTNING,
    RAW_SOURCE_VISIBLE,
)


LOGGER = get_logger(__name__)


def raw_files_for_source(settings: Settings, source_key: str) -> list[RawFileEntry]:
    raw_dir = settings.raw_dir / source_key
    files = sorted(
        raw_files_in_dir(raw_dir, source_key),
        key=lambda path: path.name,
        reverse=True,
    )
    entries: list[RawFileEntry] = []
    for file_path in files:
        stat = file_path.stat()
        entries.append(
            {
                "filename": file_path.name,
                "size_bytes": stat.st_size,
                "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=UTC).isoformat(),
                "label": f"{file_path.name} ({RAW_SOURCE_LABELS[source_key]})",
            }
        )
    return entries


def all_raw_files(settings: Settings, source_keys: tuple[str, ...]) -> list[RawFileEntry]:
    files: list[RawFileEntry] = []
    for source_key in source_keys:
        files.extend(raw_files_for_source(settings, source_key))
    files.sort(
        key=lambda item: parse_goes_timestamp(item["filename"]),
        reverse=True,
    )
    return files


def raw_timestamp_count(items: list[RawFileEntry]) -> int:
    return len({timestamp for item in items if (timestamp := parse_goes_timestamp(item["filename"]))})


def raw_disk_usage_bytes(settings: Settings, source_keys: tuple[str, ...]) -> int:
    total = 0
    for source_key in source_keys:
        for item in raw_files_for_source(settings, source_key):
            total += item["size_bytes"]
    return total


def available_raw_paths_by_source(
    settings: Settings,
    is_valid_raw: Callable[[Path], bool],
) -> dict[str, dict[str, Path]]:
    paths_by_source: dict[str, dict[str, Path]] = {}
    for source_key in (RAW_SOURCE_VISIBLE, RAW_SOURCE_INFRARED):
        entries: dict[str, Path] = {}
        for path in (settings.raw_dir / source_key).glob("*.tif"):
            if not is_valid_raw(path):
                continue
            entries[parse_goes_timestamp(path.name)] = path
        paths_by_source[source_key] = entries
    LOGGER.trace(
        "Available imagery raws by source: %s",
        {
            source_key: sorted(entries.keys(), reverse=True)
            for source_key, entries in paths_by_source.items()
        },
    )
    return paths_by_source


def available_lightning_points_by_timestamp(
    settings: Settings,
    logger: TraceLogger,
) -> dict[str, tuple[tuple[float, float], ...]]:
    points_by_timestamp: dict[str, tuple[tuple[float, float], ...]] = {}
    for path in (settings.raw_dir / RAW_SOURCE_LIGHTNING).glob("*.json"):
        timestamp = parse_goes_timestamp(path.name)
        if goes_timestamp_to_datetime(timestamp) is None:
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            logger.warning("Skipping invalid lightning cache file: %s", path.name)
            continue
        flashes = payload.get("flashes") or []
        points: list[tuple[float, float]] = []
        for item in flashes:
            if not isinstance(item, dict):
                continue
            try:
                longitude = float(item["lon"])
                latitude = float(item["lat"])
            except (KeyError, TypeError, ValueError):
                continue
            points.append((longitude, latitude))
        points_by_timestamp[timestamp] = tuple(points)
    logger.trace(
        "Loaded lightning points by timestamp: %s",
        {
            timestamp: len(points)
            for timestamp, points in sorted(points_by_timestamp.items(), reverse=True)
        },
    )
    return points_by_timestamp


def available_lightning_paths_by_timestamp(settings: Settings) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for path in (settings.raw_dir / RAW_SOURCE_LIGHTNING).glob("*.json"):
        timestamp = parse_goes_timestamp(path.name)
        if goes_timestamp_to_datetime(timestamp) is None:
            continue
        paths[timestamp] = path
    return paths


def prune_raw_cache(
    *,
    settings: Settings,
    downloaders: dict[str, GoesDownloader | GlmDownloader],
    source_keys: tuple[str, ...],
    keep_timestamps_by_source: dict[str, tuple[str, ...]],
    logger: TraceLogger,
) -> str | None:
    for source_key in source_keys:
        raw_dir = settings.raw_dir / source_key
        keep_timestamps = set(keep_timestamps_by_source[source_key])
        logger.trace(
            "Raw cache keep-set for %s: %s",
            source_key,
            sorted(keep_timestamps, reverse=True),
        )
        existing_raws = sorted(
            raw_files_in_dir(raw_dir, source_key),
            key=lambda path: (
                goes_timestamp_to_datetime(parse_goes_timestamp(path.name))
                or datetime.min.replace(tzinfo=UTC)
            ),
            reverse=True,
        )
        deleted_files: list[str] = []
        for file_path in existing_raws:
            if parse_goes_timestamp(file_path.name) not in keep_timestamps:
                deleted_files.append(file_path.name)
                file_path.unlink(missing_ok=True)

        source_dir = settings.source_dir / source_key
        for file_path in source_dir.glob("*.nc"):
            output_name = downloaders[source_key].output_filename(file_path.name)
            if parse_goes_timestamp(output_name) not in keep_timestamps:
                deleted_files.append(file_path.name)
                file_path.unlink(missing_ok=True)
        for file_path in source_dir.glob("*.nc.part"):
            source_name = file_path.name.removesuffix(".part")
            output_name = downloaders[source_key].output_filename(source_name)
            if parse_goes_timestamp(output_name) not in keep_timestamps:
                deleted_files.append(file_path.name)
                file_path.unlink(missing_ok=True)
        if deleted_files:
            logger.info(
                "Pruned %s files for %s outside retention window: %s",
                len(deleted_files),
                source_key,
                deleted_files,
            )
        else:
            logger.trace("No raw cache pruning needed for %s", source_key)

    retained_files = [
        path
        for source_key in source_keys
        for path in raw_files_in_dir(settings.raw_dir / source_key, source_key)
    ]
    return latest_kept_timestamp(retained_files)


def latest_kept_timestamp(paths: list[Path]) -> str | None:
    timestamps = [parse_goes_timestamp(path.name) for path in paths]
    usable = [timestamp for timestamp in timestamps if goes_timestamp_to_datetime(timestamp) is not None]
    if not usable:
        return None
    return max(usable)


def raw_files_in_dir(raw_dir: Path, source_key: str) -> list[Path]:
    if source_key == RAW_SOURCE_LIGHTNING:
        files = list(raw_dir.glob("*.json"))
    else:
        files = list(raw_dir.glob("*.tif"))
    if files:
        return files
    return list(raw_dir.glob("*.nc"))


def goes_timestamp_to_datetime(timestamp: str) -> datetime | None:
    if len(timestamp) < 11:
        return None
    try:
        year = int(timestamp[0:4])
        day_of_year = int(timestamp[4:7])
        hour = int(timestamp[7:9])
        minute = int(timestamp[9:11])
    except ValueError:
        return None

    moment = datetime(year, 1, 1, hour=hour, minute=minute, tzinfo=UTC)
    return moment.replace(tzinfo=UTC) + timedelta(days=day_of_year - 1)
