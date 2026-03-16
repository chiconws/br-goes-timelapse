from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
import logging
import re
from pathlib import Path
from typing import Callable
from urllib.parse import quote
import xml.etree.ElementTree as ET

import aiohttp
import rasterio

from goes_timelapse.geo2grid import BRAZIL_LONLAT_BBOX, Geo2GridConverter


LOGGER = logging.getLogger(__name__)
NODD_PRODUCT_PREFIX = "ABI-L1b-RadF"
NODD_BAND_TOKEN = "M6C02"
NODD_LISTING_PREFIX_TEMPLATE = (
    "{product}/{year}/{julian_day}/{hour}/OR_{product}-{band}_G19_"
)
NODD_KEY_PATTERN = re.compile(
    r"(?P<key>ABI-L1b-RadF/\d{4}/\d{3}/\d{2}/OR_ABI-L1b-RadF-M6C02_G19_"
    r"s(?P<timestamp>\d{13,14})_e\d{13,14}_c\d{13,14}\.nc)"
)
BOOTSTRAP_RAW_HISTORY = 3
DOWNLOAD_RETRY_ATTEMPTS = 5
DOWNLOAD_CONCURRENCY = 1
DOWNLOAD_FAILURE_BUFFER = 6
LOOKBACK_HOURS = 4
REQUEST_HEADERS = {
    "Accept-Encoding": "identity",
    "Connection": "close",
    "User-Agent": "br-goes-timelapse/0.1.0",
}
LISTING_TIMEOUT = aiohttp.ClientTimeout(total=45, connect=15, sock_connect=15, sock_read=20)
DOWNLOAD_TIMEOUT = aiohttp.ClientTimeout(total=None, connect=30, sock_connect=30, sock_read=900)
DOWNLOAD_RETRYABLE_ERRORS = (
    aiohttp.ClientConnectionError,
    aiohttp.ClientPayloadError,
    asyncio.TimeoutError,
)
S3_XML_NAMESPACE = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}


@dataclass(slots=True)
class DownloadReport:
    kept_files: list[Path]
    downloaded_count: int
    attempted_count: int
    failed_count: int
    last_downloaded: str | None
    latest_available: str | None
    failed_files: list[str] = field(default_factory=list)


class GoesDownloader:
    def __init__(
        self,
        base_url: str,
        source_dir: Path,
        raw_dir: Path,
        raw_history: int,
        *,
        converter: Geo2GridConverter | None = None,
        progress_callback: Callable[[dict[str, object]], None] | None = None,
    ):
        self._base_url = base_url.rstrip("/") + "/"
        self._source_dir = source_dir
        self._raw_dir = raw_dir
        self._raw_history = raw_history
        self._converter = converter or Geo2GridConverter()
        self._progress_callback = progress_callback
        self._source_dir.mkdir(parents=True, exist_ok=True)
        self._raw_dir.mkdir(parents=True, exist_ok=True)

    def set_ll_bbox(self, ll_bbox: tuple[float, float, float, float]) -> None:
        self._converter.set_ll_bbox(ll_bbox)

    @staticmethod
    def parse_listing(xml_payload: str) -> list[str]:
        root = ET.fromstring(xml_payload)
        keys: list[tuple[str, str]] = []
        for node in root.findall("s3:Contents", S3_XML_NAMESPACE):
            key_node = node.find("s3:Key", S3_XML_NAMESPACE)
            if key_node is None or not key_node.text:
                continue
            match = NODD_KEY_PATTERN.fullmatch(key_node.text)
            if match is None:
                continue
            keys.append((match.group("timestamp"), Path(match.group("key")).name))

        keys = sorted(set(keys), key=lambda item: item[0], reverse=True)
        return [filename for _, filename in keys]

    async def refresh_latest(self) -> DownloadReport:
        connector = aiohttp.TCPConnector(
            limit=DOWNLOAD_CONCURRENCY,
            force_close=True,
        )
        async with aiohttp.ClientSession(
            timeout=DOWNLOAD_TIMEOUT,
            connector=connector,
            headers=REQUEST_HEADERS,
        ) as session:
            try:
                source_filenames = await self._fetch_listing(session)
            except DOWNLOAD_RETRYABLE_ERRORS as err:
                source_filenames = self._cached_source_filenames_from_disk()
                if not source_filenames:
                    raise
                LOGGER.warning(
                    "Falling back to cached NODD filenames for %s: %s",
                    self._base_url,
                    err,
                )

            target_history = self._target_history()
            candidate_sources = source_filenames[: self._candidate_history(target_history)]
            candidate_outputs = [
                self._converter.output_filename(filename) for filename in candidate_sources
            ]
            self._emit_progress(
                phase="downloading" if candidate_outputs else "idle",
                attempted_count=len(candidate_outputs),
                completed_count=0,
                failed_count=0,
                active_count=0,
                latest_available=candidate_outputs[0] if candidate_outputs else None,
                current_file=None,
                last_downloaded=None,
            )
            semaphore = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)

            completed_count = 0
            failed_files: list[str] = []
            last_downloaded: str | None = None
            active_count = 0
            active_downloads: dict[str, dict[str, object]] = {}
            progress_lock = asyncio.Lock()

            async def update_progress(
                *,
                current_file: str | None = None,
                completed_delta: int = 0,
                active_delta: int = 0,
                last_downloaded_name: str | None = None,
            ) -> None:
                nonlocal completed_count, active_count, last_downloaded
                async with progress_lock:
                    completed_count += completed_delta
                    active_count = max(0, active_count + active_delta)
                    if current_file and active_delta > 0 and current_file not in active_downloads:
                        active_downloads[current_file] = {
                            "filename": current_file,
                            "downloaded_bytes": 0,
                            "total_bytes": None,
                            "percent": None,
                        }
                    if current_file and active_delta < 0:
                        active_downloads.pop(current_file, None)
                    if last_downloaded_name:
                        last_downloaded = last_downloaded_name
                    self._emit_progress(
                        phase="downloading" if active_count or completed_count < len(candidate_outputs) else "idle",
                        attempted_count=len(candidate_outputs),
                        completed_count=completed_count,
                        failed_count=len(failed_files),
                        active_count=active_count,
                        latest_available=candidate_outputs[0] if candidate_outputs else None,
                        current_file=current_file,
                        last_downloaded=last_downloaded,
                        active_downloads=sorted(
                            active_downloads.values(), key=lambda item: str(item["filename"])
                        ),
                    )

            async def update_download_bytes(
                filename: str,
                downloaded_bytes: int,
                total_bytes: int | None,
            ) -> None:
                async with progress_lock:
                    active_download = active_downloads.get(filename)
                    if active_download is None:
                        return
                    active_download["downloaded_bytes"] = downloaded_bytes
                    active_download["total_bytes"] = total_bytes
                    active_download["percent"] = (
                        round((downloaded_bytes / total_bytes) * 100, 1)
                        if total_bytes
                        else None
                    )
                    self._emit_progress(
                        phase="downloading",
                        attempted_count=len(candidate_outputs),
                        completed_count=completed_count,
                        failed_count=len(failed_files),
                        active_count=active_count,
                        latest_available=candidate_outputs[0] if candidate_outputs else None,
                        current_file=filename,
                        last_downloaded=last_downloaded,
                        active_downloads=sorted(
                            active_downloads.values(), key=lambda item: str(item["filename"])
                        ),
                    )

            async def download(source_filename: str) -> int:
                output_filename = self._converter.output_filename(source_filename)
                async with semaphore:
                    await update_progress(current_file=output_filename, active_delta=1)
                    try:
                        downloaded = await self._download_if_missing(
                            session,
                            source_filename,
                            progress_hook=update_download_bytes,
                        )
                    except Exception as err:
                        failed_files.append(output_filename)
                        active_downloads.pop(output_filename, None)
                        LOGGER.warning("Failed to download %s: %s", source_filename, err)
                        await update_progress(
                            completed_delta=1,
                            active_delta=-1,
                            current_file=output_filename,
                        )
                        return 0

                    await update_progress(
                        completed_delta=1,
                        active_delta=-1,
                        current_file=output_filename,
                        last_downloaded_name=output_filename if downloaded else None,
                    )
                    return downloaded

            results = await asyncio.gather(*(download(name) for name in candidate_sources))

        keep_filenames = [
            self._converter.output_filename(source_filename)
            for source_filename in candidate_sources
            if (self._raw_dir / self._converter.output_filename(source_filename)).exists()
        ][:target_history]
        self._cleanup_old_files(keep_filenames)
        kept_files = [
            self._raw_dir / filename
            for filename in keep_filenames
            if (self._raw_dir / filename).exists()
        ]
        return DownloadReport(
            kept_files=kept_files,
            downloaded_count=sum(results),
            attempted_count=len(candidate_outputs),
            failed_count=len(failed_files),
            last_downloaded=last_downloaded,
            latest_available=candidate_outputs[0] if candidate_outputs else None,
            failed_files=failed_files,
        )

    async def _fetch_listing(self, session: aiohttp.ClientSession) -> list[str]:
        collected: set[str] = set()
        for prefix in self._listing_prefixes():
            encoded_prefix = quote(prefix, safe="/")
            url = f"{self._base_url}?prefix={encoded_prefix}&max-keys=1000"
            for attempt in range(1, DOWNLOAD_RETRY_ATTEMPTS + 1):
                try:
                    async with session.get(url, timeout=LISTING_TIMEOUT) as response:
                        response.raise_for_status()
                        payload = await response.text()
                    collected.update(self.parse_listing(payload))
                    break
                except DOWNLOAD_RETRYABLE_ERRORS as err:
                    if attempt >= DOWNLOAD_RETRY_ATTEMPTS:
                        raise
                    LOGGER.warning(
                        "Transient failure fetching listing from %s (attempt %s/%s): %s",
                        url,
                        attempt,
                        DOWNLOAD_RETRY_ATTEMPTS,
                        err,
                    )
                    await asyncio.sleep(attempt)

        return sorted(collected, key=_filename_timestamp_or_min, reverse=True)

    async def _download_if_missing(
        self,
        session: aiohttp.ClientSession,
        filename: str,
        *,
        progress_hook: Callable[[str, int, int | None], asyncio.Future | None] | None = None,
    ) -> int:
        output_filename = self._converter.output_filename(filename)
        destination = self._raw_dir / output_filename
        if destination.exists() and self._is_expected_brazil_tiff(destination):
            return 0
        if destination.exists():
            destination.unlink(missing_ok=True)

        source_path = self._source_dir / filename
        temporary_path = source_path.with_suffix(source_path.suffix + ".part")
        source_key = self._source_key_for_filename(filename)
        for attempt in range(1, DOWNLOAD_RETRY_ATTEMPTS + 1):
            temporary_path.unlink(missing_ok=True)
            source_path.unlink(missing_ok=True)
            destination.unlink(missing_ok=True)
            try:
                async with session.get(f"{self._base_url}{source_key}") as response:
                    response.raise_for_status()
                    total_bytes = _int_or_none(response.headers.get("Content-Length"))
                    downloaded_bytes = 0
                    with temporary_path.open("wb") as handle:
                        async for chunk in response.content.iter_chunked(1024 * 128):
                            handle.write(chunk)
                            downloaded_bytes += len(chunk)
                            if progress_hook is not None:
                                await progress_hook(output_filename, downloaded_bytes, total_bytes)

                temporary_path.replace(source_path)
                await asyncio.to_thread(self._converter.convert, source_path, destination)
                source_path.unlink(missing_ok=True)
                LOGGER.info("Downloaded NOAA raw frame %s as %s", filename, output_filename)
                return 1
            except DOWNLOAD_RETRYABLE_ERRORS as err:
                temporary_path.unlink(missing_ok=True)
                source_path.unlink(missing_ok=True)
                destination.unlink(missing_ok=True)
                if attempt >= DOWNLOAD_RETRY_ATTEMPTS:
                    raise
                LOGGER.warning(
                    "Transient failure downloading %s (attempt %s/%s): %s",
                    filename,
                    attempt,
                    DOWNLOAD_RETRY_ATTEMPTS,
                    err,
                )
                await asyncio.sleep(attempt)
            except Exception:
                temporary_path.unlink(missing_ok=True)
                source_path.unlink(missing_ok=True)
                destination.unlink(missing_ok=True)
                raise

        return 0

    @staticmethod
    def _is_expected_brazil_tiff(path: Path) -> bool:
        try:
            with rasterio.open(path) as dataset:
                left, bottom, right, top = dataset.bounds
        except Exception:
            return False

        expected_left, expected_bottom, expected_right, expected_top = BRAZIL_LONLAT_BBOX
        tolerance = 1.0
        return (
            left <= expected_left + tolerance
            and bottom <= expected_bottom + tolerance
            and right >= expected_right - tolerance
            and top >= expected_top - tolerance
        )

    def _cleanup_old_files(self, keep_filenames: list[str]) -> None:
        keep = set(keep_filenames)
        for file_path in self._raw_dir.glob("*.tif"):
            if file_path.name not in keep:
                file_path.unlink(missing_ok=True)

        for file_path in self._source_dir.glob("*.nc"):
            if self._converter.output_filename(file_path.name) not in keep:
                file_path.unlink(missing_ok=True)

    def _emit_progress(self, **payload: object) -> None:
        if self._progress_callback is None:
            return
        self._progress_callback(payload)

    def _target_history(self) -> int:
        if any(self._raw_dir.glob("*.tif")):
            return self._raw_history
        return min(self._raw_history, BOOTSTRAP_RAW_HISTORY)

    def _candidate_history(self, target_history: int) -> int:
        if target_history < self._raw_history:
            return target_history
        return target_history + DOWNLOAD_FAILURE_BUFFER

    def _cached_source_filenames_from_disk(self) -> list[str]:
        return sorted(
            (
                self._converter.source_filename(path.name)
                for path in self._raw_dir.glob("*.tif")
            ),
            key=_filename_timestamp_or_min,
            reverse=True,
        )

    def _listing_prefixes(self) -> list[str]:
        prefixes: list[str] = []
        now = _floor_to_slot(_utc_now() - timedelta(minutes=10))
        seen: set[str] = set()
        for hour_offset in range(LOOKBACK_HOURS):
            hour = now - timedelta(hours=hour_offset)
            prefix = NODD_LISTING_PREFIX_TEMPLATE.format(
                product=NODD_PRODUCT_PREFIX,
                band=NODD_BAND_TOKEN,
                year=hour.strftime("%Y"),
                julian_day=hour.strftime("%j"),
                hour=hour.strftime("%H"),
            )
            if prefix in seen:
                continue
            seen.add(prefix)
            prefixes.append(prefix)
        return prefixes

    @staticmethod
    def _source_key_for_filename(filename: str) -> str:
        match = _filename_timestamp(filename)
        if match is None:
            raise ValueError(f"Cannot derive source key for {filename}")
        source_time = match.strftime("%Y/%j/%H")
        return f"{NODD_PRODUCT_PREFIX}/{source_time}/{filename}"


def _int_or_none(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


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


def _floor_to_slot(moment: datetime) -> datetime:
    floored_minute = moment.minute - (moment.minute % 10)
    return moment.replace(minute=floored_minute, second=0, microsecond=0)


def _utc_now() -> datetime:
    return datetime.now(UTC)
