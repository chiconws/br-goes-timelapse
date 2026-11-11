"""NOAA listing and partial downloads of ABI imagery and GLM flashes."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import re
import socket
from time import perf_counter
from typing import Any, Protocol
from urllib.parse import quote
import xml.etree.ElementTree as ET

import aiohttp
from typing_extensions import TypedDict

from goes_timelapse.core.logging_utils import get_logger
from goes_timelapse.pipeline.partial_read import (
    GLM_LAT_NODE,
    GLM_LON_NODE,
    GLM_NODE_SIZE,
    RETRYABLE_STATUS,
    BBox,
    CropStats,
    decode_glm_chunks,
    glm_read_ranges,
    parse_glm_node,
    read_glm_flashes_with_hdf5,
    valid_flash_points,
    write_abi_crop,
)

LOGGER = get_logger(__name__)
NODD_PRODUCT_PREFIX = "ABI-L1b-RadF"
NODD_LISTING_PREFIX_TEMPLATE = (
    "{product}/{year}/{julian_day}/{hour}/OR_{product}-{band}_G19_"
)
GLM_PRODUCT_PREFIX = "GLM-L2-LCFA"
GLM_LISTING_PREFIX_TEMPLATE = "{product}/{year}/{julian_day}/{hour}/OR_{product}_G19_"
DOWNLOAD_RETRY_ATTEMPTS = 5
DOWNLOAD_CONCURRENCY = 2
GLM_CONCURRENCY = 8
GLM_LAYOUT_CHECK_SECONDS = 3600
LOOKBACK_HOURS = 4
REQUEST_HEADERS = {
    "Accept-Encoding": "identity",
    "User-Agent": "br-goes-timelapse/1.2.0",
}
LISTING_TIMEOUT = aiohttp.ClientTimeout(
    total=45, connect=15, sock_connect=15, sock_read=20
)
DOWNLOAD_TIMEOUT = aiohttp.ClientTimeout(
    total=None, connect=30, sock_connect=30, sock_read=900
)


class TransientHttpError(aiohttp.ClientError):
    """S3 asked to retry, as in 503 Slow Down."""


DOWNLOAD_RETRYABLE_ERRORS = (
    aiohttp.ClientConnectionError,
    aiohttp.ClientPayloadError,
    asyncio.TimeoutError,
    TransientHttpError,
)
S3_XML_NAMESPACE = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}


class SessionLike(Protocol):
    """Minimal aiohttp session interface used by the downloaders."""

    def get(self, url: Any, **kwargs: Any) -> Any:
        """Start a GET request."""


class GlmSlotRecord(TypedDict):
    """Flashes aggregated for one GLM slot."""

    slot_timestamp: str
    source_filenames: list[str]
    flashes: list[dict[str, float]]


@dataclass(slots=True)
class DownloadReport:
    """Outcome of one refresh of a source."""

    kept_files: list[Path]
    downloaded_count: int
    attempted_count: int
    failed_count: int
    last_downloaded: str | None
    latest_available: str | None
    failed_files: list[str] = field(default_factory=list)
    bytes_read: int = 0
    requests: int = 0
    retries: int = 0
    listing_seconds: float = 0.0
    elapsed_seconds: float = 0.0


@dataclass(slots=True, frozen=True)
class AreaCrop:
    """Destination crop of one municipality."""

    area_id: str
    bounds: BBox
    target_timestamps: tuple[str, ...]


class GoesDownloader:
    """Read the rows of ABI C02/C13 that cover each municipality."""

    def __init__(
        self,
        base_url: str,
        raw_dir: Path,
        raw_history: int,
        *,
        band: str = "C02",
        progress_callback: Callable[[dict[str, object]], None] | None = None,
    ):
        """Configure the product, cache directory and limits."""
        self._base_url = base_url.rstrip("/") + "/"
        self._raw_dir = raw_dir
        self._raw_history = raw_history
        self._band = band
        self._band_token = f"M6{band}"
        self._key_pattern = re.compile(
            rf"(?P<key>{re.escape(NODD_PRODUCT_PREFIX)}/\d{{4}}/\d{{3}}/\d{{2}}/"
            rf"OR_{re.escape(NODD_PRODUCT_PREFIX)}-{re.escape(self._band_token)}_G19_"
            rf"s(?P<timestamp>\d{{13,14}})_e\d{{13,14}}_c\d{{13,14}}\.nc)"
        )
        self._progress_callback = progress_callback
        self._raw_dir.mkdir(parents=True, exist_ok=True)

    def output_filename(self, source_filename: str) -> str:
        """Return the cached file name for a NOAA file name."""
        return f"{Path(source_filename).stem}.tif"

    def source_filename(self, output_filename: str) -> str:
        """Return the NOAA file name for a cached file name."""
        return f"{Path(output_filename).stem}.nc"

    def parse_listing(self, xml_payload: str) -> list[str]:
        """Return the product file names in an S3 listing."""
        root = ET.fromstring(xml_payload)  # noqa: S314 - NOAA S3 listing fetched over HTTPS
        keys: list[tuple[str, str]] = []
        for node in root.findall("s3:Contents", S3_XML_NAMESPACE):
            key_node = node.find("s3:Key", S3_XML_NAMESPACE)
            if key_node is None or not key_node.text:
                continue
            match = self._key_pattern.fullmatch(key_node.text)
            if match is None:
                continue
            keys.append((match.group("timestamp"), Path(match.group("key")).name))

        keys = sorted(set(keys), key=lambda item: item[0], reverse=True)
        return [filename for _, filename in keys]

    async def refresh_latest(
        self,
        *,
        download_missing: bool = True,
        target_timestamps: tuple[str, ...] | None = None,
        areas: tuple[AreaCrop, ...] = (),
    ) -> DownloadReport:
        """Fetch the missing slots for every crop."""
        started_at = perf_counter()
        connector = aiohttp.TCPConnector(
            limit=DOWNLOAD_CONCURRENCY, family=socket.AF_INET
        )
        slots_on_disk = {
            area.area_id: self._slots_on_disk(area.area_id) for area in areas
        }
        missing_slots = tuple(
            {
                slot
                for area in areas
                for slot in area.target_timestamps
                if slot not in slots_on_disk[area.area_id]
            }
        )
        async with aiohttp.ClientSession(
            timeout=LISTING_TIMEOUT,
            connector=connector,
            headers=REQUEST_HEADERS,
        ) as session:
            try:
                source_filenames = await self._fetch_listing(session, missing_slots)
            except DOWNLOAD_RETRYABLE_ERRORS as err:
                source_filenames = self._cached_source_filenames_from_disk()
                if not source_filenames:
                    raise
                LOGGER.warning(
                    "Falling back to cached NODD filenames for %s: %s",
                    self._base_url,
                    err,
                )
        listing_seconds = perf_counter() - started_at

        candidate_sources = self._candidate_source_filenames(
            source_filenames,
            target_timestamps=target_timestamps,
        )
        candidate_outputs = [
            self.output_filename(filename) for filename in candidate_sources
        ]
        latest_available = self._latest_available_output(
            source_filenames,
            target_timestamps=target_timestamps,
        )
        LOGGER.trace(
            "Imagery refresh plan for %s: targets=%s candidates=%s latest_available=%s",
            self._band,
            target_timestamps,
            candidate_outputs,
            latest_available,
        )
        reads = [
            (source_filename, area)
            for source_filename in candidate_sources
            for area in areas
            if (
                slot := _slot_timestamp_for_output_filename(
                    self.output_filename(source_filename)
                )
            )
            in area.target_timestamps
            and slot not in slots_on_disk[area.area_id]
        ]
        attempted_count = len(reads)
        self._emit_progress(
            phase="downloading" if download_missing and reads else "idle",
            attempted_count=attempted_count,
            completed_count=0,
            failed_count=0,
            active_count=0,
            latest_available=latest_available,
            current_file=None,
            last_downloaded=None,
        )
        if not download_missing:
            return DownloadReport(
                kept_files=self._kept_raws_on_disk(),
                downloaded_count=0,
                attempted_count=0,
                failed_count=0,
                last_downloaded=None,
                latest_available=latest_available,
                failed_files=[],
            )

        failed_files: list[str] = []
        downloaded_count = 0
        bytes_read = 0
        requests = 0
        retries = 0
        last_downloaded: str | None = None
        # ponytail: one file at a time, each with its own connections; add file-level concurrency if backfill is too slow.
        for completed_count, (source_filename, area) in enumerate(reads):
            output_filename = f"{area.area_id}/{self.output_filename(source_filename)}"
            self._emit_progress(
                phase="downloading",
                attempted_count=attempted_count,
                completed_count=completed_count,
                failed_count=len(failed_files),
                active_count=1,
                latest_available=latest_available,
                current_file=output_filename,
                last_downloaded=last_downloaded,
                active_downloads=[
                    {
                        "filename": output_filename,
                        "downloaded_bytes": 0,
                        "total_bytes": None,
                        "percent": None,
                        "stage": "downloading",
                    }
                ],
            )
            try:
                stats = await asyncio.to_thread(self._read_crop, source_filename, area)
            except Exception as err:  # noqa: BLE001 - one bad file must not stop the cycle
                # The slot stays missing, so the next cycle tries it again.
                failed_files.append(output_filename)
                LOGGER.warning("Failed to read %s: %s", source_filename, err)
                continue
            downloaded_count += 1
            bytes_read += stats.bytes_read
            requests += stats.requests
            retries += stats.retries
            last_downloaded = output_filename

        return DownloadReport(
            kept_files=self._kept_raws_on_disk(),
            downloaded_count=downloaded_count,
            attempted_count=attempted_count,
            failed_count=len(failed_files),
            last_downloaded=last_downloaded,
            latest_available=latest_available,
            failed_files=failed_files,
            bytes_read=bytes_read,
            requests=requests,
            retries=retries,
            listing_seconds=listing_seconds,
            elapsed_seconds=perf_counter() - started_at,
        )

    def _read_crop(self, filename: str, area: AreaCrop) -> CropStats:
        stats = write_abi_crop(
            f"{self._base_url}{self._source_key_for_filename(filename)}",
            self._band,
            self._raw_dir / area.area_id / self.output_filename(filename),
            area.bounds,
            user_agent=REQUEST_HEADERS["User-Agent"],
        )
        LOGGER.info(
            "METRIC crop area=%s band=%s slot=%s bytes=%s requests=%s retries=%s rows=%s "
            "tiff_bytes=%s network_s=%.2f decode_s=%.2f reproject_s=%.2f "
            "write_s=%.2f total_s=%.2f mb_per_s=%.2f",
            area.area_id,
            self._band,
            _slot_timestamp_for_output_filename(filename),
            stats.bytes_read,
            stats.requests,
            stats.retries,
            stats.rows,
            stats.tiff_bytes,
            stats.network_seconds,
            stats.decode_seconds,
            stats.reproject_seconds,
            stats.write_seconds,
            stats.total_seconds,
            stats.bytes_read / 1e6 / max(stats.network_seconds, 1e-6),
        )
        return stats

    def _slots_on_disk(self, area_id: str) -> set[str | None]:
        return {
            _slot_timestamp_for_output_filename(path.name)
            for path in (self._raw_dir / area_id).glob("*.tif")
            if path.stat().st_size > 0
        }

    async def _fetch_listing(
        self,
        session: SessionLike,
        missing_slots: tuple[str, ...] = (),
    ) -> list[str]:
        collected: set[str] = set()
        last_error: Exception | None = None
        for prefix in self._listing_prefixes(missing_slots):
            encoded_prefix = quote(prefix, safe="/")
            url = f"{self._base_url}?prefix={encoded_prefix}&max-keys=1000"
            LOGGER.trace("Fetching imagery listing prefix for %s: %s", self._band, url)
            for attempt in range(1, DOWNLOAD_RETRY_ATTEMPTS + 1):
                try:
                    async with session.get(url, timeout=LISTING_TIMEOUT) as response:
                        _raise_for_response(response)
                        payload = await response.text()
                    parsed = self.parse_listing(payload)
                    LOGGER.trace(
                        "Fetched imagery listing prefix for %s: %s file(s)",
                        self._band,
                        len(parsed),
                    )
                    collected.update(parsed)
                    break
                except DOWNLOAD_RETRYABLE_ERRORS as err:
                    last_error = err
                    if attempt >= DOWNLOAD_RETRY_ATTEMPTS:
                        LOGGER.warning(
                            "Skipping listing prefix after repeated failures %s: %s",
                            url,
                            err,
                        )
                        break
                    LOGGER.warning(
                        "Transient failure fetching listing from %s (attempt %s/%s): %s",
                        url,
                        attempt,
                        DOWNLOAD_RETRY_ATTEMPTS,
                        err,
                    )
                    await asyncio.sleep(attempt)

        if collected:
            return sorted(collected, key=_filename_timestamp_or_min, reverse=True)
        if last_error is not None:
            raise last_error
        return sorted(collected, key=_filename_timestamp_or_min, reverse=True)

    def _emit_progress(self, **payload: object) -> None:
        if self._progress_callback is None:
            return
        self._progress_callback(payload)

    def _candidate_source_filenames(
        self,
        source_filenames: list[str],
        *,
        target_timestamps: tuple[str, ...] | None,
    ) -> list[str]:
        if target_timestamps is None:
            selected = source_filenames[: self._raw_history]
            LOGGER.trace(
                "Selected imagery candidates for %s without explicit targets: %s",
                self._band,
                [self.output_filename(filename) for filename in selected],
            )
            return selected
        target_set = set(target_timestamps)
        selected = [
            filename
            for filename in source_filenames
            if _slot_timestamp_for_output_filename(self.output_filename(filename))
            in target_set
        ]
        LOGGER.trace(
            "Selected imagery candidates for %s with targets=%s: %s",
            self._band,
            target_timestamps,
            [self.output_filename(filename) for filename in selected],
        )
        return selected

    def _latest_available_output(
        self,
        source_filenames: list[str],
        *,
        target_timestamps: tuple[str, ...] | None,
    ) -> str | None:
        if target_timestamps is None:
            return (
                self.output_filename(source_filenames[0]) if source_filenames else None
            )
        target_set = set(target_timestamps)
        for filename in source_filenames:
            output_filename = self.output_filename(filename)
            if _slot_timestamp_for_output_filename(output_filename) in target_set:
                return output_filename
        return None

    def _cached_source_filenames_from_disk(self) -> list[str]:
        return sorted(
            {self.source_filename(path.name) for path in self._raw_dir.rglob("*.tif")},
            key=_filename_timestamp_or_min,
            reverse=True,
        )

    def _kept_raws_on_disk(self) -> list[Path]:
        return sorted(
            self._raw_dir.rglob("*.tif"),
            key=lambda path: _filename_timestamp_or_min(path.name),
            reverse=True,
        )

    def _listing_prefixes(self, missing_slots: tuple[str, ...] = ()) -> list[str]:
        return [
            NODD_LISTING_PREFIX_TEMPLATE.format(
                product=NODD_PRODUCT_PREFIX,
                band=self._band_token,
                year=hour.strftime("%Y"),
                julian_day=hour.strftime("%j"),
                hour=hour.strftime("%H"),
            )
            for hour in _listing_hours(missing_slots)
        ]

    @staticmethod
    def _source_key_for_filename(filename: str) -> str:
        match = _filename_timestamp(filename)
        if match is None:
            raise ValueError(f"Cannot derive source key for {filename}")
        source_time = match.strftime("%Y/%j/%H")
        return f"{NODD_PRODUCT_PREFIX}/{source_time}/{filename}"


class GlmDownloader:
    """Read GLM flash coordinates and aggregate them per slot."""

    def __init__(
        self,
        base_url: str,
        raw_dir: Path,
        raw_history: int,
        *,
        progress_callback: Callable[[dict[str, object]], None] | None = None,
    ):
        """Configure the cache directory and limits."""
        self._base_url = base_url.rstrip("/") + "/"
        self._raw_dir = raw_dir
        self._raw_history = raw_history
        self._progress_callback = progress_callback
        self._bytes_read = 0
        self._requests = 0
        self._retries = 0
        self._layout_checked_at = float("-inf")
        self._key_pattern = re.compile(
            rf"(?P<key>{re.escape(GLM_PRODUCT_PREFIX)}/\d{{4}}/\d{{3}}/\d{{2}}/"
            rf"OR_{re.escape(GLM_PRODUCT_PREFIX)}_G19_"
            rf"s(?P<timestamp>\d{{13,14}})_e\d{{13,14}}_c\d{{13,14}}\.nc)"
        )
        self._raw_dir.mkdir(parents=True, exist_ok=True)

    def output_filename(self, source_filename: str) -> str:
        """Return the cached slot file name for a NOAA file name."""
        slot_timestamp = self._slot_timestamp_for_source(source_filename)
        if slot_timestamp is None:
            raise ValueError(f"Cannot derive GLM slot for {source_filename}")
        return f"{slot_timestamp}_glm.json"

    def parse_listing(self, xml_payload: str) -> list[str]:
        """Return the GLM file names in an S3 listing."""
        root = ET.fromstring(xml_payload)  # noqa: S314 - NOAA S3 listing fetched over HTTPS
        keys: list[tuple[str, str]] = []
        for node in root.findall("s3:Contents", S3_XML_NAMESPACE):
            key_node = node.find("s3:Key", S3_XML_NAMESPACE)
            if key_node is None or not key_node.text:
                continue
            match = self._key_pattern.fullmatch(key_node.text)
            if match is None:
                continue
            keys.append((match.group("timestamp"), Path(match.group("key")).name))

        keys = sorted(set(keys), key=lambda item: item[0], reverse=True)
        return [filename for _, filename in keys]

    async def refresh_latest(
        self,
        *,
        download_missing: bool = True,
        target_timestamps: tuple[str, ...] | None = None,
    ) -> DownloadReport:
        """Fetch and aggregate flashes for the missing slots."""
        started_at = perf_counter()
        connector = aiohttp.TCPConnector(limit=GLM_CONCURRENCY, family=socket.AF_INET)
        self._bytes_read = 0
        self._requests = 0
        self._retries = 0
        missing_slots = tuple(
            slot
            for slot in target_timestamps or ()
            if not (self._raw_dir / f"{slot}_glm.json").exists()
        )
        async with aiohttp.ClientSession(
            timeout=DOWNLOAD_TIMEOUT,
            connector=connector,
            headers=REQUEST_HEADERS,
        ) as session:
            source_filenames = await self._fetch_listing(session, missing_slots)
            listing_seconds = perf_counter() - started_at
            candidate_sources, latest_slot_output = self._candidate_source_filenames(
                source_filenames,
                target_timestamps=target_timestamps,
            )
            LOGGER.trace(
                "GLM refresh plan: targets=%s candidates=%s latest_available=%s",
                target_timestamps,
                candidate_sources,
                latest_slot_output,
            )
            self._emit_progress(
                phase="downloading"
                if download_missing and candidate_sources
                else "idle",
                attempted_count=len(candidate_sources),
                completed_count=0,
                failed_count=0,
                active_count=0,
                latest_available=latest_slot_output,
                current_file=None,
                last_downloaded=None,
            )

            if not download_missing:
                kept_files = self._kept_raws_on_disk()
                return DownloadReport(
                    kept_files=kept_files,
                    downloaded_count=0,
                    attempted_count=0,
                    failed_count=0,
                    last_downloaded=None,
                    latest_available=latest_slot_output,
                    failed_files=[],
                )

            if candidate_sources and not await self._layout_verified(
                session, candidate_sources[0]
            ):
                return DownloadReport(
                    kept_files=self._kept_raws_on_disk(),
                    downloaded_count=0,
                    attempted_count=len(candidate_sources),
                    failed_count=len(candidate_sources),
                    last_downloaded=None,
                    latest_available=latest_slot_output,
                    failed_files=candidate_sources,
                )

            download_semaphore = asyncio.Semaphore(GLM_CONCURRENCY)
            completed_count = 0
            failed_files: list[str] = []
            last_downloaded: str | None = None
            active_downloads: dict[str, dict[str, object]] = {}
            progress_lock = asyncio.Lock()
            ready_for_conversion: dict[
                int, tuple[str, str, list[dict[str, float]] | None]
            ] = {}
            ready_for_conversion_event = asyncio.Event()

            def current_phase() -> str:
                if any(
                    str(item.get("stage")) == "downloading"
                    for item in active_downloads.values()
                ):
                    return "downloading"
                if active_downloads:
                    return "processing"
                if completed_count < len(candidate_sources):
                    return "downloading"
                return "idle"

            async def upsert_active_download(
                *,
                filename: str,
                stage: str,
                downloaded_bytes: int | None = None,
                total_bytes: int | None = None,
                percent: float | None = None,
            ) -> None:
                async with progress_lock:
                    item = active_downloads.get(filename)
                    if item is None:
                        item = {
                            "filename": filename,
                            "downloaded_bytes": 0,
                            "total_bytes": None,
                            "percent": None,
                            "stage": stage,
                        }
                        active_downloads[filename] = item
                    else:
                        item["stage"] = stage
                    if downloaded_bytes is not None:
                        item["downloaded_bytes"] = downloaded_bytes
                    if total_bytes is not None or stage == "downloading":
                        item["total_bytes"] = total_bytes
                    if percent is not None:
                        item["percent"] = percent
                    self._emit_progress(
                        phase=current_phase(),
                        attempted_count=len(candidate_sources),
                        completed_count=completed_count,
                        failed_count=len(failed_files),
                        active_count=len(active_downloads),
                        latest_available=latest_slot_output,
                        current_file=filename,
                        last_downloaded=last_downloaded,
                        active_downloads=sorted(
                            active_downloads.values(),
                            key=lambda item: str(item["filename"]),
                        ),
                    )

            async def complete_active_download(
                *,
                filename: str,
                last_downloaded_name: str | None = None,
            ) -> None:
                nonlocal completed_count, last_downloaded
                async with progress_lock:
                    completed_count += 1
                    active_downloads.pop(filename, None)
                    if last_downloaded_name:
                        last_downloaded = last_downloaded_name
                    self._emit_progress(
                        phase=current_phase(),
                        attempted_count=len(candidate_sources),
                        completed_count=completed_count,
                        failed_count=len(failed_files),
                        active_count=len(active_downloads),
                        latest_available=latest_slot_output,
                        current_file=filename,
                        last_downloaded=last_downloaded,
                        active_downloads=sorted(
                            active_downloads.values(),
                            key=lambda item: str(item["filename"]),
                        ),
                    )

            async def _mark_ready_for_conversion(
                source_index: int,
                source_filename: str,
                slot_output: str,
                flashes: list[dict[str, float]] | None,
            ) -> None:
                ready_for_conversion[source_index] = (
                    source_filename,
                    slot_output,
                    flashes,
                )
                ready_for_conversion_event.set()

            async def download(source_index: int, filename: str) -> None:
                slot_output = self.output_filename(filename)
                await upsert_active_download(filename=filename, stage="downloading")
                try:
                    async with download_semaphore:
                        flashes = await self._read_flash_columns(
                            session,
                            self._source_key_for_filename(filename),
                        )

                    await upsert_active_download(filename=filename, stage="queued")
                    await _mark_ready_for_conversion(
                        source_index,
                        filename,
                        slot_output,
                        flashes,
                    )
                except Exception as err:  # noqa: BLE001 - one bad file must not stop the cycle
                    failed_files.append(filename)
                    LOGGER.warning("Failed to download %s: %s", filename, err)
                    await complete_active_download(filename=filename)
                    await _mark_ready_for_conversion(
                        source_index,
                        filename,
                        slot_output,
                        None,
                    )

            download_tasks = [
                asyncio.create_task(download(index, name))
                for index, name in enumerate(candidate_sources)
            ]

            downloaded_count = 0
            for index in range(len(candidate_sources)):
                while index not in ready_for_conversion:
                    await ready_for_conversion_event.wait()
                    ready_for_conversion_event.clear()

                source_filename, slot_output, flashes = ready_for_conversion.pop(index)
                if flashes is None:
                    continue

                await upsert_active_download(
                    filename=source_filename, stage="converting"
                )
                try:
                    processed = self._merge_flashes_into_slot(source_filename, flashes)
                    downloaded_count += processed
                    await complete_active_download(
                        filename=source_filename,
                        last_downloaded_name=slot_output if processed else None,
                    )
                except Exception as err:  # noqa: BLE001 - one bad file must not stop the cycle
                    failed_files.append(source_filename)
                    LOGGER.warning("Failed to convert %s: %s", source_filename, err)
                    await complete_active_download(filename=source_filename)

            await asyncio.gather(*download_tasks)

        kept_files = self._kept_raws_on_disk()
        return DownloadReport(
            kept_files=kept_files,
            downloaded_count=downloaded_count,
            attempted_count=len(candidate_sources),
            failed_count=len(failed_files),
            last_downloaded=last_downloaded,
            latest_available=latest_slot_output,
            failed_files=failed_files,
            bytes_read=self._bytes_read,
            requests=self._requests,
            retries=self._retries,
            listing_seconds=listing_seconds,
            elapsed_seconds=perf_counter() - started_at,
        )

    async def _layout_verified(self, session: SessionLike, filename: str) -> bool:
        """False stops GLM until the check passes, since a moved node yields valid-looking flashes."""
        if perf_counter() - self._layout_checked_at < GLM_LAYOUT_CHECK_SECONDS:
            return True
        source_key = self._source_key_for_filename(filename)
        try:
            direct = await self._read_flash_columns(session, source_key)
            reference = await asyncio.to_thread(
                read_glm_flashes_with_hdf5,
                f"{self._base_url}{source_key}",
                user_agent=REQUEST_HEADERS["User-Agent"],
            )
        except Exception as err:  # noqa: BLE001 - any failure makes the check inconclusive
            LOGGER.warning("GLM layout check inconclusive for %s: %s", filename, err)
            return False
        if direct != reference:
            LOGGER.error(
                "GLM layout changed, stopping GLM reads: mismatch in %s", filename
            )
            return False
        self._layout_checked_at = perf_counter()
        return True

    async def _read_flash_columns(
        self,
        session: SessionLike,
        source_key: str,
    ) -> list[dict[str, float]]:
        latitude_chunks = parse_glm_node(
            await self._get_range(
                session, source_key, GLM_LAT_NODE, GLM_LAT_NODE + GLM_NODE_SIZE
            )
        )
        longitude_chunks = parse_glm_node(
            await self._get_range(
                session, source_key, GLM_LON_NODE, GLM_LON_NODE + GLM_NODE_SIZE
            )
        )
        ranges = {
            (start, end): await self._get_range(session, source_key, start, end)
            for start, end in glm_read_ranges(latitude_chunks + longitude_chunks)
        }
        return valid_flash_points(
            decode_glm_chunks(latitude_chunks, ranges),
            decode_glm_chunks(longitude_chunks, ranges),
        )

    async def _get_range(
        self,
        session: SessionLike,
        source_key: str,
        start: int,
        end: int,
    ) -> bytes:
        headers = {"Range": f"bytes={start}-{end - 1}"}
        for attempt in range(1, DOWNLOAD_RETRY_ATTEMPTS + 1):
            try:
                async with session.get(
                    f"{self._base_url}{source_key}",
                    headers=headers,
                    timeout=LISTING_TIMEOUT,
                ) as response:
                    _raise_for_response(response)
                    if response.status != 206:
                        raise ValueError(f"Range ignorado: HTTP {response.status}")
                    data = await response.read()
                if len(data) != end - start:
                    raise ValueError(
                        f"Leitura curta: {len(data)} de {end - start} bytes"
                    )
                self._bytes_read += len(data)
                self._requests += 1
                return data  # noqa: TRY300 - return inside the retry loop
            except DOWNLOAD_RETRYABLE_ERRORS:
                if attempt >= DOWNLOAD_RETRY_ATTEMPTS:
                    raise
                self._retries += 1
                await asyncio.sleep(attempt)
        raise AssertionError("unreachable")

    async def _fetch_listing(
        self,
        session: SessionLike,
        missing_slots: tuple[str, ...] = (),
    ) -> list[str]:
        collected: set[str] = set()
        last_error: Exception | None = None
        for prefix in self._listing_prefixes(missing_slots):
            encoded_prefix = quote(prefix, safe="/")
            url = f"{self._base_url}?prefix={encoded_prefix}&max-keys=1000"
            LOGGER.trace("Fetching GLM listing prefix: %s", url)
            for attempt in range(1, DOWNLOAD_RETRY_ATTEMPTS + 1):
                try:
                    async with session.get(url, timeout=LISTING_TIMEOUT) as response:
                        _raise_for_response(response)
                        payload = await response.text()
                    parsed = self.parse_listing(payload)
                    LOGGER.trace(
                        "Fetched GLM listing prefix with %s file(s)", len(parsed)
                    )
                    collected.update(parsed)
                    break
                except DOWNLOAD_RETRYABLE_ERRORS as err:
                    last_error = err
                    if attempt >= DOWNLOAD_RETRY_ATTEMPTS:
                        LOGGER.warning(
                            "Skipping listing prefix after repeated failures %s: %s",
                            url,
                            err,
                        )
                        break
                    LOGGER.warning(
                        "Transient failure fetching listing from %s (attempt %s/%s): %s",
                        url,
                        attempt,
                        DOWNLOAD_RETRY_ATTEMPTS,
                        err,
                    )
                    await asyncio.sleep(attempt)

        if collected:
            return sorted(collected, key=_filename_timestamp_or_min, reverse=True)
        if last_error is not None:
            raise last_error
        return []

    def _merge_flashes_into_slot(
        self, filename: str, flashes: list[dict[str, float]]
    ) -> int:
        output_filename = self.output_filename(filename)
        destination = self._raw_dir / output_filename
        record = self._load_slot_record(destination)
        if filename in record["source_filenames"]:
            LOGGER.trace(
                "Skipping GLM merge because source already exists in slot %s: %s",
                output_filename,
                filename,
            )
            return 0

        record["source_filenames"].append(filename)
        record["source_filenames"].sort(reverse=True)
        record["flashes"].extend(flashes)
        self._write_slot_record(destination, record)
        LOGGER.trace(
            "Merged GLM source %s into %s with %s flash(es)",
            filename,
            output_filename,
            len(flashes),
        )
        return 1

    @staticmethod
    def _load_slot_record(path: Path) -> GlmSlotRecord:
        if not path.exists():
            return {
                "slot_timestamp": parse_goes_slot_prefix(path.name) or "",
                "source_filenames": [],
                "flashes": [],
            }
        payload = json.loads(path.read_text(encoding="utf-8"))
        return {
            "slot_timestamp": str(
                payload.get("slot_timestamp") or parse_goes_slot_prefix(path.name)
            ),
            "source_filenames": [
                str(name) for name in (payload.get("source_filenames") or [])
            ],
            "flashes": [
                {"lat": float(item["lat"]), "lon": float(item["lon"])}
                for item in (payload.get("flashes") or [])
                if isinstance(item, dict)
                and isinstance(item.get("lat"), int | float)
                and isinstance(item.get("lon"), int | float)
            ],
        }

    @staticmethod
    def _write_slot_record(path: Path, record: GlmSlotRecord) -> None:
        temporary_path = path.with_suffix(".json.tmp")
        temporary_path.write_text(
            json.dumps(record, ensure_ascii=True, separators=(",", ":")),
            encoding="utf-8",
        )
        temporary_path.replace(path)

    def _candidate_source_filenames(
        self,
        source_filenames: list[str],
        *,
        target_timestamps: tuple[str, ...] | None,
    ) -> tuple[list[str], str | None]:
        selected_slots: list[str]
        selected_slot_set: set[str]
        if target_timestamps is None:
            selected_slots = []
            selected_slot_set = set()
            target_history = self._raw_history
        else:
            selected_slots = list(target_timestamps)
            selected_slot_set = set(selected_slots)
            target_history = len(selected_slots)

        candidate_sources: list[str] = []
        processed_by_slot: dict[str, set[str]] = {}

        for filename in source_filenames:
            slot_timestamp = self._slot_timestamp_for_source(filename)
            if slot_timestamp is None:
                continue
            if target_timestamps is None and slot_timestamp not in selected_slot_set:
                if len(selected_slots) >= target_history:
                    break
                selected_slots.append(slot_timestamp)
                selected_slot_set.add(slot_timestamp)
            elif (
                target_timestamps is not None
                and slot_timestamp not in selected_slot_set
            ):
                continue
            processed = processed_by_slot.get(slot_timestamp)
            if processed is None:
                record = self._load_slot_record(
                    self._raw_dir / f"{slot_timestamp}_glm.json"
                )
                processed = {str(name) for name in record["source_filenames"]}
                processed_by_slot[slot_timestamp] = processed
            if filename in processed:
                continue
            candidate_sources.append(filename)

        latest_slot_output = None
        if selected_slots:
            if target_timestamps is None:
                latest_slot_output = f"{selected_slots[0]}_glm.json"
            else:
                available_slots = {
                    self._slot_timestamp_for_source(filename)
                    for filename in source_filenames
                }
                for slot_timestamp in selected_slots:
                    if slot_timestamp in available_slots:
                        latest_slot_output = f"{slot_timestamp}_glm.json"
                        break
        LOGGER.trace(
            "Selected GLM candidates: selected_slots=%s candidates=%s latest_slot_output=%s",
            selected_slots,
            candidate_sources,
            latest_slot_output,
        )
        return candidate_sources, latest_slot_output

    def _kept_raws_on_disk(self) -> list[Path]:
        return sorted(
            self._raw_dir.glob("*.json"),
            key=lambda path: _filename_timestamp_or_min(path.name),
            reverse=True,
        )

    def _listing_prefixes(self, missing_slots: tuple[str, ...] = ()) -> list[str]:
        return [
            GLM_LISTING_PREFIX_TEMPLATE.format(
                product=GLM_PRODUCT_PREFIX,
                year=hour.strftime("%Y"),
                julian_day=hour.strftime("%j"),
                hour=hour.strftime("%H"),
            )
            for hour in _listing_hours(missing_slots)
        ]

    def _source_key_for_filename(self, filename: str) -> str:
        match = _filename_timestamp(filename)
        if match is None:
            raise ValueError(f"Cannot derive source key for {filename}")
        source_time = match.strftime("%Y/%j/%H")
        return f"{GLM_PRODUCT_PREFIX}/{source_time}/{filename}"

    def _slot_timestamp_for_source(self, filename: str) -> str | None:
        source_time = _filename_timestamp(filename)
        if source_time is None:
            return None
        return _datetime_to_slot_timestamp(_floor_to_slot(source_time))

    def _emit_progress(self, **payload: object) -> None:
        if self._progress_callback is None:
            return
        self._progress_callback(payload)


def _listing_hours(missing_slots: tuple[str, ...]) -> list[datetime]:
    """Recent hours plus the ones a backfill needs; listing all 24 every poll would be wasteful."""
    now = _floor_to_slot(_utc_now() - timedelta(minutes=10))
    recent = (now - timedelta(hours=offset) for offset in range(LOOKBACK_HOURS))
    backfill = (
        datetime.strptime(slot, "%Y%j%H%M").replace(tzinfo=UTC)
        for slot in missing_slots
    )
    hours: dict[str, datetime] = {}
    for moment in (*recent, *backfill):
        hours.setdefault(moment.strftime("%Y%j%H"), moment)
    return list(hours.values())


def _raise_for_response(response: Any) -> None:
    if response.status in RETRYABLE_STATUS:
        raise TransientHttpError(f"HTTP {response.status}")
    response.raise_for_status()


def _filename_timestamp(filename: str) -> datetime | None:
    match = re.search(r"_s(\d{13,14})_", filename)
    if match is None:
        return None
    value = match.group(1)[:13]
    try:
        return datetime.strptime(value, "%Y%j%H%M%S").replace(tzinfo=UTC)
    except ValueError:
        return None


def _filename_timestamp_or_min(filename: str) -> datetime:
    return _filename_timestamp(filename) or datetime.min.replace(tzinfo=UTC)


def _datetime_to_slot_timestamp(moment: datetime) -> str:
    return moment.strftime("%Y%j%H%M")


def _slot_timestamp_for_output_filename(filename: str) -> str | None:
    timestamp = _filename_timestamp(filename)
    if timestamp is not None:
        return _datetime_to_slot_timestamp(_floor_to_slot(timestamp))
    return parse_goes_slot_prefix(filename)


def parse_goes_slot_prefix(filename: str) -> str | None:
    """Return the slot timestamp of a GOES file name, or None."""
    prefix = Path(filename).name.split("_", 1)[0]
    if len(prefix) < 11 or not prefix[:11].isdigit():
        return None
    return prefix[:11]


def _floor_to_slot(moment: datetime) -> datetime:
    floored_minute = moment.minute - (moment.minute % 10)
    return moment.replace(minute=floored_minute, second=0, microsecond=0)


def _utc_now() -> datetime:
    return datetime.now(UTC)
