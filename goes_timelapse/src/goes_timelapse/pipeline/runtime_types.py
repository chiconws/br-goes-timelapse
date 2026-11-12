"""Typed records shared by the refresh pipeline."""

from __future__ import annotations

from dataclasses import dataclass

from typing_extensions import TypedDict

RAW_SOURCE_VISIBLE = "visible"
RAW_SOURCE_INFRARED = "infrared"
RAW_SOURCE_LIGHTNING = "lightning"

RAW_SOURCE_LABELS = {
    RAW_SOURCE_VISIBLE: "Visível B2",
    RAW_SOURCE_INFRARED: "Infravermelho B13",
    RAW_SOURCE_LIGHTNING: "Descargas GLM",
}


@dataclass(slots=True, frozen=True)
class DownloadSourcePlan:
    """What one source should download this cycle, and why."""

    source_key: str
    source_label: str
    tracked_area_ids: tuple[str, ...]
    should_download: bool
    target_timestamps: tuple[str, ...]
    reason: str


class ActiveDownloadEntry(TypedDict):
    """A file being read right now."""

    filename: str


class SourceDownloadStatus(TypedDict):
    """Download state of one source."""

    source_key: str
    phase: str
    attempted_count: int
    completed_count: int
    failed_count: int
    active_count: int
    active_downloads: list[ActiveDownloadEntry]
    schedule_reason: str
    is_relevant: bool


class RawFileEntry(TypedDict):
    """A cached raw file with its timestamp."""

    filename: str
    size_bytes: int
    modified_at: str
    label: str
