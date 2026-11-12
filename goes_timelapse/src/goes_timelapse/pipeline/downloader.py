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
from typing import Any
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
from goes_timelapse.pipeline.runtime_types import SLOT_MINUTES
from goes_timelapse.timeline.plan import datetime_to_slot_timestamp, floor_to_slot

LOGGER = get_logger(__name__)
ABI_PRODUCT = "ABI-L1b-RadF"
GLM_PRODUCT = "GLM-L2-LCFA"
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


class _NoaaDownloader:
    """S3 listing and progress shared by the ABI and GLM downloaders."""

    def __init__(
        self,
        base_url: str,
        raw_dir: Path,
        *,
        product: str,
        name_prefix: str,
        progress_callback: Callable[[dict[str, object]], None] | None,
    ):
        self._base_url = base_url.rstrip("/") + "/"
        self._raw_dir = raw_dir
        self._product = product
        self._name_prefix = name_prefix
        self._key_pattern = re.compile(
            rf"(?P<key>{re.escape(product)}/\d{{4}}/\d{{3}}/\d{{2}}/"
            rf"{re.escape(name_prefix)}"
            rf"s(?P<timestamp>\d{{13,14}})_e\d{{13,14}}_c\d{{13,14}}\.nc)"
        )
        self._progress_callback = progress_callback
        self._raw_dir.mkdir(parents=True, exist_ok=True)

    def parse_listing(self, xml_payload: str) -> list[str]:
        """Return the product file names in an S3 listing, newest first."""
        root = ET.fromstring(xml_payload)  # noqa: S314 - NOAA S3 listing fetched over HTTPS
        keys: set[tuple[str, str]] = set()
        for node in root.findall("s3:Contents", S3_XML_NAMESPACE):
            key_node = node.find("s3:Key", S3_XML_NAMESPACE)
            if key_node is None or not key_node.text:
                continue
            match = self._key_pattern.fullmatch(key_node.text)
            if match is not None:
                keys.add((match.group("timestamp"), Path(match.group("key")).name))
        return [filename for _, filename in sorted(keys, reverse=True)]

    async def _fetch_listing(
        self,
        session: aiohttp.ClientSession,
        missing_slots: tuple[str, ...] = (),
    ) -> list[str]:
        collected: set[str] = set()
        last_error: Exception | None = None
        for hour in _listing_hours(missing_slots):
            prefix = f"{self._product}/{hour:%Y/%j/%H}/{self._name_prefix}"
            url = f"{self._base_url}?prefix={quote(prefix, safe='/')}&max-keys=1000"
            LOGGER.trace("Fetching listing prefix: %s", url)
            for attempt in range(1, DOWNLOAD_RETRY_ATTEMPTS + 1):
                try:
                    async with session.get(url, timeout=LISTING_TIMEOUT) as response:
                        _raise_for_response(response)
                        payload = await response.text()
                    collected.update(self.parse_listing(payload))
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

        if not collected and last_error is not None:
            raise last_error
        return sorted(collected, key=_filename_timestamp_or_min, reverse=True)

    def _source_key_for_filename(self, filename: str) -> str:
        moment = _filename_timestamp(filename)
        if moment is None:
            raise ValueError(f"Cannot derive source key for {filename}")
        return f"{self._product}/{moment:%Y/%j/%H}/{filename}"

    def _emit_progress(self, **payload: object) -> None:
        if self._progress_callback is not None:
            self._progress_callback(payload)


class GoesDownloader(_NoaaDownloader):
    """Read the rows of ABI C02/C13 that cover each municipality."""

    def __init__(
        self,
        base_url: str,
        raw_dir: Path,
        *,
        band: str = "C02",
        progress_callback: Callable[[dict[str, object]], None] | None = None,
    ):
        """Configure the band and cache directory."""
        super().__init__(
            base_url,
            raw_dir,
            product=ABI_PRODUCT,
            name_prefix=f"OR_{ABI_PRODUCT}-M6{band}_G19_",
            progress_callback=progress_callback,
        )
        self._band = band

    def output_filename(self, source_filename: str) -> str:
        """Return the cached file name for a NOAA file name."""
        return f"{Path(source_filename).stem}.tif"

    async def refresh_latest(
        self,
        *,
        target_timestamps: tuple[str, ...],
        areas: tuple[AreaCrop, ...] = (),
    ) -> DownloadReport:
        """Fetch the missing slots for every crop."""
        started_at = perf_counter()
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
            connector=aiohttp.TCPConnector(
                limit=DOWNLOAD_CONCURRENCY, family=socket.AF_INET
            ),
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

        target_set = set(target_timestamps)
        reads = [
            (filename, area)
            for filename in source_filenames
            if (slot := _slot_of(filename)) in target_set
            for area in areas
            if slot in area.target_timestamps
            and slot not in slots_on_disk[area.area_id]
        ]
        LOGGER.trace("Imagery reads for %s: %s", self._band, reads)
        self._emit_progress(
            phase="downloading" if reads else "idle",
            attempted_count=len(reads),
            completed_count=0,
            failed_count=0,
            active_count=0,
            active_downloads=[],
        )

        failed_files: list[str] = []
        downloaded_count = 0
        bytes_read = 0
        requests = 0
        retries = 0
        # ponytail: one file at a time, each with its own connections; add file-level concurrency if backfill is too slow.
        for completed_count, (source_filename, area) in enumerate(reads):
            output_filename = f"{area.area_id}/{self.output_filename(source_filename)}"
            self._emit_progress(
                phase="downloading",
                attempted_count=len(reads),
                completed_count=completed_count,
                failed_count=len(failed_files),
                active_count=1,
                active_downloads=[{"filename": output_filename}],
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

        return DownloadReport(
            kept_files=sorted(
                self._raw_dir.rglob("*.tif"),
                key=lambda path: _filename_timestamp_or_min(path.name),
                reverse=True,
            ),
            downloaded_count=downloaded_count,
            attempted_count=len(reads),
            failed_count=len(failed_files),
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
            _slot_of(filename),
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
            _slot_of(path.name)
            for path in (self._raw_dir / area_id).glob("*.tif")
            if path.stat().st_size > 0
        }

    def _cached_source_filenames_from_disk(self) -> list[str]:
        return sorted(
            {f"{path.stem}.nc" for path in self._raw_dir.rglob("*.tif")},
            key=_filename_timestamp_or_min,
            reverse=True,
        )


class GlmDownloader(_NoaaDownloader):
    """Read GLM flash coordinates and aggregate them per slot."""

    def __init__(
        self,
        base_url: str,
        raw_dir: Path,
        *,
        progress_callback: Callable[[dict[str, object]], None] | None = None,
    ):
        """Configure the cache directory."""
        super().__init__(
            base_url,
            raw_dir,
            product=GLM_PRODUCT,
            name_prefix=f"OR_{GLM_PRODUCT}_G19_",
            progress_callback=progress_callback,
        )
        self._bytes_read = 0
        self._requests = 0
        self._retries = 0
        self._layout_checked_at = float("-inf")

    async def refresh_latest(
        self, *, target_timestamps: tuple[str, ...]
    ) -> DownloadReport:
        """Fetch and aggregate flashes for the missing slots."""
        started_at = perf_counter()
        self._bytes_read = 0
        self._requests = 0
        self._retries = 0
        missing_slots = tuple(
            slot for slot in target_timestamps if not self._slot_path(slot).exists()
        )
        async with aiohttp.ClientSession(
            timeout=DOWNLOAD_TIMEOUT,
            connector=aiohttp.TCPConnector(
                limit=GLM_CONCURRENCY, family=socket.AF_INET
            ),
            headers=REQUEST_HEADERS,
        ) as session:
            source_filenames = await self._fetch_listing(session, missing_slots)
            listing_seconds = perf_counter() - started_at
            candidates = self._candidate_source_filenames(
                source_filenames, set(target_timestamps)
            )
            LOGGER.trace("GLM candidates for %s: %s", target_timestamps, candidates)
            self._emit_progress(
                phase="downloading" if candidates else "idle",
                attempted_count=len(candidates),
                completed_count=0,
                failed_count=0,
                active_count=0,
                active_downloads=[],
            )
            if candidates and not await self._layout_verified(session, candidates[0]):
                return DownloadReport(
                    kept_files=self._kept_raws_on_disk(),
                    downloaded_count=0,
                    attempted_count=len(candidates),
                    failed_count=len(candidates),
                    failed_files=candidates,
                )

            semaphore = asyncio.Semaphore(GLM_CONCURRENCY)
            failed_files: list[str] = []
            completed_count = 0
            in_flight: list[str] = []

            def emit() -> None:
                self._emit_progress(
                    phase="downloading",
                    attempted_count=len(candidates),
                    completed_count=completed_count,
                    failed_count=len(failed_files),
                    active_count=len(in_flight),
                    active_downloads=[{"filename": name} for name in in_flight],
                )

            async def read(filename: str) -> list[dict[str, float]] | None:
                nonlocal completed_count
                try:
                    async with semaphore:
                        in_flight.append(filename)
                        emit()
                        return await self._read_flash_columns(
                            session, self._source_key_for_filename(filename)
                        )
                except Exception as err:  # noqa: BLE001 - one bad file must not stop the cycle
                    failed_files.append(filename)
                    LOGGER.warning("Failed to download %s: %s", filename, err)
                    return None
                finally:
                    if filename in in_flight:
                        in_flight.remove(filename)
                    completed_count += 1
                    emit()

            results = await asyncio.gather(*(read(name) for name in candidates))

        downloaded_count = 0
        for filename, flashes in zip(candidates, results, strict=True):
            if flashes is None:
                continue
            try:
                downloaded_count += self._merge_flashes_into_slot(filename, flashes)
            except Exception as err:  # noqa: BLE001 - one bad file must not stop the cycle
                failed_files.append(filename)
                LOGGER.warning("Failed to convert %s: %s", filename, err)

        return DownloadReport(
            kept_files=self._kept_raws_on_disk(),
            downloaded_count=downloaded_count,
            attempted_count=len(candidates),
            failed_count=len(failed_files),
            failed_files=failed_files,
            bytes_read=self._bytes_read,
            requests=self._requests,
            retries=self._retries,
            listing_seconds=listing_seconds,
            elapsed_seconds=perf_counter() - started_at,
        )

    async def _layout_verified(
        self, session: aiohttp.ClientSession, filename: str
    ) -> bool:
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
        session: aiohttp.ClientSession,
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
        session: aiohttp.ClientSession,
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
                    data: bytes = await response.read()
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

    def _candidate_source_filenames(
        self, source_filenames: list[str], target_slots: set[str]
    ) -> list[str]:
        processed_by_slot: dict[str, set[str]] = {}
        candidates: list[str] = []
        for filename in source_filenames:
            slot = _slot_of(filename)
            if slot is None or slot not in target_slots:
                continue
            if slot not in processed_by_slot:
                record = self._load_slot_record(slot)
                processed_by_slot[slot] = set(record["source_filenames"])
            if filename not in processed_by_slot[slot]:
                candidates.append(filename)
        return candidates

    def _merge_flashes_into_slot(
        self, filename: str, flashes: list[dict[str, float]]
    ) -> int:
        slot = _slot_of(filename)
        if slot is None:
            raise ValueError(f"Cannot derive GLM slot for {filename}")
        record = self._load_slot_record(slot)
        if filename in record["source_filenames"]:
            return 0
        record["source_filenames"].append(filename)
        record["source_filenames"].sort(reverse=True)
        record["flashes"].extend(flashes)
        destination = self._slot_path(slot)
        temporary_path = destination.with_suffix(".json.tmp")
        temporary_path.write_text(
            json.dumps(record, ensure_ascii=True, separators=(",", ":")),
            encoding="utf-8",
        )
        temporary_path.replace(destination)
        LOGGER.trace(
            "Merged GLM source %s into slot %s with %s flash(es)",
            filename,
            slot,
            len(flashes),
        )
        return 1

    def _slot_path(self, slot: str) -> Path:
        return self._raw_dir / f"{slot}_glm.json"

    def _load_slot_record(self, slot: str) -> GlmSlotRecord:
        path = self._slot_path(slot)
        if not path.exists():
            return {"slot_timestamp": slot, "source_filenames": [], "flashes": []}
        payload = json.loads(path.read_text(encoding="utf-8"))
        return {
            "slot_timestamp": slot,
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

    def _kept_raws_on_disk(self) -> list[Path]:
        return sorted(self._raw_dir.glob("*.json"), reverse=True)


def _listing_hours(missing_slots: tuple[str, ...]) -> list[datetime]:
    """Recent hours plus the ones a backfill needs; listing all 24 every poll would be wasteful."""
    now = floor_to_slot(
        _utc_now() - timedelta(minutes=SLOT_MINUTES), slot_minutes=SLOT_MINUTES
    )
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
    match = re.search(r"_s(\d{13})", filename)
    if match is None:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y%j%H%M%S").replace(tzinfo=UTC)
    except ValueError:
        return None


def _filename_timestamp_or_min(filename: str) -> datetime:
    return _filename_timestamp(filename) or datetime.min.replace(tzinfo=UTC)


def _slot_of(filename: str) -> str | None:
    """Return the 10-minute slot a NOAA or cached file name starts in."""
    moment = _filename_timestamp(filename)
    if moment is None:
        return None
    return datetime_to_slot_timestamp(floor_to_slot(moment, slot_minutes=SLOT_MINUTES))


def _utc_now() -> datetime:
    return datetime.now(UTC)
