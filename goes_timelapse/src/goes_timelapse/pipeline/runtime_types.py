from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing_extensions import TypedDict

RAW_SOURCE_VISIBLE = "visible"
RAW_SOURCE_INFRARED = "infrared"
RAW_SOURCE_LIGHTNING = "lightning"

RAW_SOURCE_LABELS = {
    RAW_SOURCE_VISIBLE: "Visível B2",
    RAW_SOURCE_INFRARED: "Infravermelho B13",
    RAW_SOURCE_LIGHTNING: "Descargas GLM",
}

SLOT_MINUTES = 10

CACHE_WARNING_FREE_BYTES = 20 * 1024 * 1024 * 1024
CACHE_BLOCKING_FREE_BYTES = 2 * 1024 * 1024 * 1024
STAGING_WARNING_FREE_BYTES = 8 * 1024 * 1024 * 1024
STAGING_BLOCKING_FREE_BYTES = 2 * 1024 * 1024 * 1024


@dataclass(slots=True, frozen=True)
class DownloadSourcePlan:
    source_key: str
    source_label: str
    tracked_area_ids: tuple[str, ...]
    should_download: bool
    target_timestamps: tuple[str, ...]
    reason: str


@dataclass(slots=True, frozen=True)
class StorageCheck:
    key: str
    label: str
    path: Path
    free_bytes: int
    total_bytes: int
    warning: str | None
    is_blocking: bool


class ActiveDownloadEntry(TypedDict):
    filename: str
    downloaded_bytes: int
    total_bytes: int | None
    percent: float | None
    stage: str


class SourceDownloadStatus(TypedDict):
    source_key: str
    phase: str
    attempted_count: int
    completed_count: int
    failed_count: int
    active_count: int
    current_file: str | None
    last_downloaded: str | None
    latest_available: str | None
    active_downloads: list[ActiveDownloadEntry]
    schedule_reason: str
    is_relevant: bool


class RawFileEntry(TypedDict):
    filename: str
    size_bytes: int
    modified_at: str
    label: str


class GlmFlashPoint(TypedDict):
    lat: float
    lon: float


class GlmSlotRecord(TypedDict):
    slot_timestamp: str
    source_filenames: list[str]
    flashes: list[GlmFlashPoint]
