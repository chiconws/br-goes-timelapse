"""Cached raw frames: listing, lookup and pruning."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

from goes_timelapse.core.config import Settings
from goes_timelapse.core.logging_utils import get_logger
from goes_timelapse.pipeline.rendering import parse_goes_timestamp
from goes_timelapse.pipeline.runtime_types import (
    RAW_SOURCE_INFRARED,
    RAW_SOURCE_LABELS,
    RAW_SOURCE_LIGHTNING,
    RAW_SOURCE_VISIBLE,
    RawFileEntry,
)

LOGGER = get_logger(__name__)


def raw_files_for_source(settings: Settings, source_key: str) -> list[RawFileEntry]:
    """List the cached files of a source with their timestamps."""
    raw_dir = settings.raw_dir / source_key
    files = sorted(
        raw_files_in_dir(raw_dir, source_key),
        key=lambda path: path.name,
        reverse=True,
    )
    entries: list[RawFileEntry] = []
    for file_path in files:
        stat = file_path.stat()
        filename = file_path.relative_to(raw_dir).as_posix()
        entries.append(
            {
                "filename": filename,
                "size_bytes": stat.st_size,
                "modified_at": datetime.fromtimestamp(
                    stat.st_mtime, tz=UTC
                ).isoformat(),
                "label": f"{filename} ({RAW_SOURCE_LABELS[source_key]})",
            }
        )
    return entries


RAW_SOURCES = (RAW_SOURCE_VISIBLE, RAW_SOURCE_INFRARED, RAW_SOURCE_LIGHTNING)


def all_raw_files(settings: Settings) -> list[RawFileEntry]:
    """List the cached files of every source."""
    files: list[RawFileEntry] = []
    for source_key in RAW_SOURCES:
        files.extend(raw_files_for_source(settings, source_key))
    files.sort(
        key=lambda item: parse_goes_timestamp(item["filename"]),
        reverse=True,
    )
    return files


def raw_timestamp_count(items: list[RawFileEntry]) -> int:
    """Count distinct timestamps among cached files."""
    return len(
        {
            timestamp
            for item in items
            if (timestamp := parse_goes_timestamp(item["filename"]))
        }
    )


def available_raw_paths_by_source(
    settings: Settings,
    area_id: str,
    is_valid_raw: Callable[[Path], bool],
) -> dict[str, dict[str, Path]]:
    """Map each source to its cached files by timestamp."""
    paths_by_source: dict[str, dict[str, Path]] = {}
    for source_key in (RAW_SOURCE_VISIBLE, RAW_SOURCE_INFRARED):
        entries: dict[str, Path] = {}
        for path in (settings.raw_dir / source_key / area_id).glob("*.tif"):
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
) -> dict[str, tuple[tuple[float, float], ...]]:
    """Load the cached GLM flash points per slot."""
    points_by_timestamp: dict[str, tuple[tuple[float, float], ...]] = {}
    for path in (settings.raw_dir / RAW_SOURCE_LIGHTNING).glob("*.json"):
        timestamp = parse_goes_timestamp(path.name)
        if goes_timestamp_to_datetime(timestamp) is None:
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            LOGGER.warning("Skipping invalid lightning cache file: %s", path.name)
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
    LOGGER.trace(
        "Loaded lightning points by timestamp: %s",
        {
            timestamp: len(points)
            for timestamp, points in sorted(points_by_timestamp.items(), reverse=True)
        },
    )
    return points_by_timestamp


def prune_raw_cache(
    *,
    settings: Settings,
    keep_timestamps_by_source: dict[str, tuple[str, ...]],
) -> str | None:
    """Delete cached files outside the timeline; return an error message, if any."""
    for source_key in RAW_SOURCES:
        raw_dir = settings.raw_dir / source_key
        keep_timestamps = set(keep_timestamps_by_source[source_key])
        LOGGER.trace(
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

        if deleted_files:
            LOGGER.info(
                "Pruned %s files for %s outside retention window: %s",
                len(deleted_files),
                source_key,
                deleted_files,
            )
        else:
            LOGGER.trace("No raw cache pruning needed for %s", source_key)

    retained_files = [
        path
        for source_key in RAW_SOURCES
        for path in raw_files_in_dir(settings.raw_dir / source_key, source_key)
    ]
    return latest_kept_timestamp(retained_files)


def latest_kept_timestamp(paths: list[Path]) -> str | None:
    """Return the newest valid timestamp among paths."""
    timestamps = [parse_goes_timestamp(path.name) for path in paths]
    usable = [
        timestamp
        for timestamp in timestamps
        if goes_timestamp_to_datetime(timestamp) is not None
    ]
    if not usable:
        return None
    return max(usable)


def raw_files_in_dir(raw_dir: Path, source_key: str) -> list[Path]:
    """List the cached files of a source under raw_dir."""
    if source_key == RAW_SOURCE_LIGHTNING:
        return list(raw_dir.glob("*.json"))
    # Direct children are files from older versions; pruning clears them as they age out.
    return [*raw_dir.rglob("*.tif"), *raw_dir.glob("*.nc")]


def goes_timestamp_to_datetime(timestamp: str) -> datetime | None:
    """Parse a GOES 'sYYYYJJJHHMM' timestamp, or None if invalid."""
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
