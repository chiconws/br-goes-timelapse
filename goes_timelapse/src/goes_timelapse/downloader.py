from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
import json
import logging
import re
from pathlib import Path
from typing import Callable
from urllib.parse import quote
import xml.etree.ElementTree as ET

import aiohttp
import netCDF4
import rasterio

from goes_timelapse.geo2grid import BRAZIL_LONLAT_BBOX, Geo2GridConverter


LOGGER = logging.getLogger(__name__)
NODD_PRODUCT_PREFIX = "ABI-L1b-RadF"
NODD_LISTING_PREFIX_TEMPLATE = (
    "{product}/{year}/{julian_day}/{hour}/OR_{product}-{band}_G19_"
)
GLM_PRODUCT_PREFIX = "GLM-L2-LCFA"
GLM_LISTING_PREFIX_TEMPLATE = "{product}/{year}/{julian_day}/{hour}/OR_{product}_G19_"
BOOTSTRAP_RAW_HISTORY = 3
DOWNLOAD_RETRY_ATTEMPTS = 5
DOWNLOAD_CONCURRENCY = 2
CONVERT_CONCURRENCY = 1
LOOKBACK_HOURS = 4
REQUEST_HEADERS = {
    "Accept-Encoding": "identity",
    "Connection": "close",
    "User-Agent": "br-goes-timelapse/1.1.0",
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
        band: str = "C02",
        scratch_dir: Path | None = None,
        converter: Geo2GridConverter | None = None,
        progress_callback: Callable[[dict[str, object]], None] | None = None,
    ):
        self._base_url = base_url.rstrip("/") + "/"
        self._source_dir = source_dir
        self._raw_dir = raw_dir
        self._raw_history = raw_history
        self._band = band
        self._band_token = f"M6{band}"
        self._key_pattern = re.compile(
            rf"(?P<key>{re.escape(NODD_PRODUCT_PREFIX)}/\d{{4}}/\d{{3}}/\d{{2}}/"
            rf"OR_{re.escape(NODD_PRODUCT_PREFIX)}-{re.escape(self._band_token)}_G19_"
            rf"s(?P<timestamp>\d{{13,14}})_e\d{{13,14}}_c\d{{13,14}}\.nc)"
        )
        self._converter = converter or Geo2GridConverter(
            product=band,
            scratch_dir=scratch_dir,
        )
        self._progress_callback = progress_callback
        self._source_dir.mkdir(parents=True, exist_ok=True)
        self._raw_dir.mkdir(parents=True, exist_ok=True)

    def output_filename(self, source_filename: str) -> str:
        return self._converter.output_filename(source_filename)

    def source_filename(self, output_filename: str) -> str:
        return self._converter.source_filename(output_filename)

    def set_ll_bbox(self, ll_bbox: tuple[float, float, float, float]) -> None:
        self._converter.set_ll_bbox(ll_bbox)

    def parse_listing(self, xml_payload: str) -> list[str]:
        root = ET.fromstring(xml_payload)
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

    async def refresh_latest(self, *, download_missing: bool = True) -> DownloadReport:
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
            candidate_sources = source_filenames[:target_history]
            candidate_outputs = [
                self.output_filename(filename) for filename in candidate_sources
            ]
            self._emit_progress(
                phase="downloading" if download_missing and candidate_outputs else "idle",
                attempted_count=len(candidate_outputs),
                completed_count=0,
                failed_count=0,
                active_count=0,
                latest_available=candidate_outputs[0] if candidate_outputs else None,
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
                    latest_available=candidate_outputs[0] if candidate_outputs else None,
                    failed_files=[],
                )

            download_semaphore = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)
            convert_semaphore = asyncio.Semaphore(CONVERT_CONCURRENCY)

            completed_count = 0
            failed_files: list[str] = []
            last_downloaded: str | None = None
            active_downloads: dict[str, dict[str, object]] = {}
            progress_lock = asyncio.Lock()

            def current_phase() -> str:
                if any(
                    str(item.get("stage")) == "downloading"
                    for item in active_downloads.values()
                ):
                    return "downloading"
                if active_downloads:
                    return "processing"
                if completed_count < len(candidate_outputs):
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
                        attempted_count=len(candidate_outputs),
                        completed_count=completed_count,
                        failed_count=len(failed_files),
                        active_count=len(active_downloads),
                        latest_available=candidate_outputs[0] if candidate_outputs else None,
                        current_file=filename,
                        last_downloaded=last_downloaded,
                        active_downloads=sorted(
                            active_downloads.values(), key=lambda item: str(item["filename"])
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
                        attempted_count=len(candidate_outputs),
                        completed_count=completed_count,
                        failed_count=len(failed_files),
                        active_count=len(active_downloads),
                        latest_available=candidate_outputs[0] if candidate_outputs else None,
                        current_file=filename,
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
                percent = (
                    round((downloaded_bytes / total_bytes) * 100, 1)
                    if total_bytes
                    else None
                )
                await upsert_active_download(
                    filename=filename,
                    stage="downloading",
                    downloaded_bytes=downloaded_bytes,
                    total_bytes=total_bytes,
                    percent=percent,
                )

            async def download(source_filename: str) -> int:
                output_filename = self._converter.output_filename(source_filename)
                await upsert_active_download(filename=output_filename, stage="downloading")
                try:
                    async with download_semaphore:
                        source_path = await self._download_source_if_needed(
                            session,
                            source_filename,
                            progress_hook=update_download_bytes,
                        )
                    if source_path is None:
                        await complete_active_download(filename=output_filename)
                        return 0

                    await upsert_active_download(
                        filename=output_filename,
                        stage="converting",
                    )
                    async with convert_semaphore:
                        converted = await self._convert_source_to_tiff(
                            source_filename,
                            source_path,
                        )
                    await complete_active_download(
                        filename=output_filename,
                        last_downloaded_name=output_filename if converted else None,
                    )
                    return converted
                except Exception as err:
                    failed_files.append(output_filename)
                    LOGGER.warning("Failed to download %s: %s", source_filename, err)
                    await complete_active_download(filename=output_filename)
                    return 0

            results = await asyncio.gather(*(download(name) for name in candidate_sources))

        kept_files = self._kept_raws_on_disk()
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
        last_error: Exception | None = None
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

    async def _download_source_if_needed(
        self,
        session: aiohttp.ClientSession,
        filename: str,
        *,
        progress_hook: Callable[[str, int, int | None], asyncio.Future | None] | None = None,
    ) -> Path | None:
        output_filename = self.output_filename(filename)
        destination = self._raw_dir / output_filename
        if destination.exists() and self._is_expected_brazil_tiff(destination):
            return None
        if destination.exists():
            destination.unlink(missing_ok=True)

        source_path = self._source_dir / filename
        if source_path.exists():
            return source_path

        temporary_path = source_path.with_suffix(source_path.suffix + ".part")
        source_key = self._source_key_for_filename(filename)
        for attempt in range(1, DOWNLOAD_RETRY_ATTEMPTS + 1):
            temporary_path.unlink(missing_ok=True)
            source_path.unlink(missing_ok=True)
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
                return source_path
            except DOWNLOAD_RETRYABLE_ERRORS as err:
                temporary_path.unlink(missing_ok=True)
                source_path.unlink(missing_ok=True)
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
                raise

        return None

    async def _convert_source_to_tiff(self, filename: str, source_path: Path) -> int:
        output_filename = self.output_filename(filename)
        destination = self._raw_dir / output_filename
        if destination.exists() and self._is_expected_brazil_tiff(destination):
            source_path.unlink(missing_ok=True)
            return 0
        if destination.exists():
            destination.unlink(missing_ok=True)

        try:
            await asyncio.to_thread(self._converter.convert, source_path, destination)
            LOGGER.info("Downloaded NOAA raw frame %s as %s", filename, output_filename)
            source_path.unlink(missing_ok=True)
            return 1
        except Exception:
            source_path.unlink(missing_ok=True)
            destination.unlink(missing_ok=True)
            raise

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

    def _emit_progress(self, **payload: object) -> None:
        if self._progress_callback is None:
            return
        self._progress_callback(payload)

    def _target_history(self) -> int:
        if any(self._raw_dir.glob("*.tif")):
            return self._raw_history
        return min(self._raw_history, BOOTSTRAP_RAW_HISTORY)

    def _cached_source_filenames_from_disk(self) -> list[str]:
        return sorted(
            (
                self.source_filename(path.name)
                for path in self._raw_dir.glob("*.tif")
            ),
            key=_filename_timestamp_or_min,
            reverse=True,
        )

    def _kept_raws_on_disk(self) -> list[Path]:
        return sorted(
            self._raw_dir.glob("*.tif"),
            key=lambda path: _filename_timestamp_or_min(path.name),
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
                band=self._band_token,
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


class GlmDownloader:
    def __init__(
        self,
        base_url: str,
        source_dir: Path,
        raw_dir: Path,
        raw_history: int,
        *,
        progress_callback: Callable[[dict[str, object]], None] | None = None,
    ):
        self._base_url = base_url.rstrip("/") + "/"
        self._source_dir = source_dir
        self._raw_dir = raw_dir
        self._raw_history = raw_history
        self._progress_callback = progress_callback
        self._key_pattern = re.compile(
            rf"(?P<key>{re.escape(GLM_PRODUCT_PREFIX)}/\d{{4}}/\d{{3}}/\d{{2}}/"
            rf"OR_{re.escape(GLM_PRODUCT_PREFIX)}_G19_"
            rf"s(?P<timestamp>\d{{13,14}})_e\d{{13,14}}_c\d{{13,14}}\.nc)"
        )
        self._source_dir.mkdir(parents=True, exist_ok=True)
        self._raw_dir.mkdir(parents=True, exist_ok=True)

    def output_filename(self, source_filename: str) -> str:
        slot_timestamp = self._slot_timestamp_for_source(source_filename)
        if slot_timestamp is None:
            raise ValueError(f"Cannot derive GLM slot for {source_filename}")
        return f"{slot_timestamp}_glm.json"

    def parse_listing(self, xml_payload: str) -> list[str]:
        root = ET.fromstring(xml_payload)
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

    async def refresh_latest(self, *, download_missing: bool = True) -> DownloadReport:
        connector = aiohttp.TCPConnector(
            limit=DOWNLOAD_CONCURRENCY,
            force_close=True,
        )
        async with aiohttp.ClientSession(
            timeout=DOWNLOAD_TIMEOUT,
            connector=connector,
            headers=REQUEST_HEADERS,
        ) as session:
            source_filenames = await self._fetch_listing(session)
            candidate_sources, latest_slot_output = self._candidate_source_filenames(source_filenames)
            self._emit_progress(
                phase="downloading" if download_missing and candidate_sources else "idle",
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

            download_semaphore = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)
            convert_semaphore = asyncio.Semaphore(CONVERT_CONCURRENCY)

            completed_count = 0
            failed_files: list[str] = []
            last_downloaded: str | None = None
            active_downloads: dict[str, dict[str, object]] = {}
            progress_lock = asyncio.Lock()

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

            async def update_download_bytes(
                filename: str,
                downloaded_bytes: int,
                total_bytes: int | None,
            ) -> None:
                percent = (
                    round((downloaded_bytes / total_bytes) * 100, 1)
                    if total_bytes
                    else None
                )
                await upsert_active_download(
                    filename=filename,
                    stage="downloading",
                    downloaded_bytes=downloaded_bytes,
                    total_bytes=total_bytes,
                    percent=percent,
                )

            async def download(filename: str) -> int:
                slot_output = self.output_filename(filename)
                await upsert_active_download(filename=filename, stage="downloading")
                try:
                    async with download_semaphore:
                        source_path = await self._download_source_if_needed(
                            session,
                            filename,
                            progress_hook=update_download_bytes,
                        )
                    if source_path is None:
                        await complete_active_download(filename=filename)
                        return 0

                    await upsert_active_download(filename=filename, stage="converting")
                    async with convert_semaphore:
                        processed = await self._merge_source_into_slot(filename, source_path)
                    await complete_active_download(
                        filename=filename,
                        last_downloaded_name=slot_output if processed else None,
                    )
                    return processed
                except Exception as err:
                    failed_files.append(filename)
                    LOGGER.warning("Failed to download %s: %s", filename, err)
                    await complete_active_download(filename=filename)
                    return 0

            results = await asyncio.gather(*(download(name) for name in candidate_sources))

        kept_files = self._kept_raws_on_disk()
        return DownloadReport(
            kept_files=kept_files,
            downloaded_count=sum(results),
            attempted_count=len(candidate_sources),
            failed_count=len(failed_files),
            last_downloaded=last_downloaded,
            latest_available=latest_slot_output,
            failed_files=failed_files,
        )

    async def _fetch_listing(self, session: aiohttp.ClientSession) -> list[str]:
        collected: set[str] = set()
        last_error: Exception | None = None
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

    async def _download_source_if_needed(
        self,
        session: aiohttp.ClientSession,
        filename: str,
        *,
        progress_hook: Callable[[str, int, int | None], asyncio.Future | None] | None = None,
    ) -> Path | None:
        source_path = self._source_dir / filename
        if source_path.exists():
            return source_path

        temporary_path = source_path.with_suffix(source_path.suffix + ".part")
        source_key = self._source_key_for_filename(filename)
        for attempt in range(1, DOWNLOAD_RETRY_ATTEMPTS + 1):
            temporary_path.unlink(missing_ok=True)
            source_path.unlink(missing_ok=True)
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
                                await progress_hook(filename, downloaded_bytes, total_bytes)

                temporary_path.replace(source_path)
                return source_path
            except DOWNLOAD_RETRYABLE_ERRORS as err:
                temporary_path.unlink(missing_ok=True)
                source_path.unlink(missing_ok=True)
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
                raise

        return None

    async def _merge_source_into_slot(self, filename: str, source_path: Path) -> int:
        output_filename = self.output_filename(filename)
        destination = self._raw_dir / output_filename
        try:
            record = self._load_slot_record(destination)
            processed_sources = set(record["source_filenames"])
            if filename in processed_sources:
                source_path.unlink(missing_ok=True)
                return 0

            flashes = await asyncio.to_thread(self._extract_flash_points, source_path)
            record["source_filenames"].append(filename)
            record["source_filenames"].sort(reverse=True)
            record["flashes"].extend(flashes)
            self._write_slot_record(destination, record)
            source_path.unlink(missing_ok=True)
            return 1
        except Exception:
            source_path.unlink(missing_ok=True)
            raise

    @staticmethod
    def _extract_flash_points(source_path: Path) -> list[dict[str, float]]:
        flashes: list[dict[str, float]] = []
        with netCDF4.Dataset(source_path, "r") as dataset:
            flash_lat = dataset.variables.get("flash_lat")
            flash_lon = dataset.variables.get("flash_lon")
            if flash_lat is None or flash_lon is None:
                return flashes
            latitudes = flash_lat[:]
            longitudes = flash_lon[:]
            for lat, lon in zip(latitudes, longitudes, strict=False):
                try:
                    latitude = float(lat)
                    longitude = float(lon)
                except (TypeError, ValueError):
                    continue
                if not (-90.0 <= latitude <= 90.0 and -180.0 <= longitude <= 180.0):
                    continue
                flashes.append({"lat": latitude, "lon": longitude})
        return flashes

    @staticmethod
    def _load_slot_record(path: Path) -> dict[str, object]:
        if not path.exists():
            return {
                "slot_timestamp": parse_goes_slot_prefix(path.name),
                "source_filenames": [],
                "flashes": [],
            }
        payload = json.loads(path.read_text(encoding="utf-8"))
        return {
            "slot_timestamp": str(payload.get("slot_timestamp") or parse_goes_slot_prefix(path.name)),
            "source_filenames": list(payload.get("source_filenames") or []),
            "flashes": list(payload.get("flashes") or []),
        }

    @staticmethod
    def _write_slot_record(path: Path, record: dict[str, object]) -> None:
        temporary_path = path.with_suffix(".json.tmp")
        temporary_path.write_text(
            json.dumps(record, ensure_ascii=True, separators=(",", ":")),
            encoding="utf-8",
        )
        temporary_path.replace(path)

    def _candidate_source_filenames(self, source_filenames: list[str]) -> tuple[list[str], str | None]:
        selected_slots: list[str] = []
        selected_slot_set: set[str] = set()
        candidate_sources: list[str] = []
        processed_by_slot: dict[str, set[str]] = {}
        target_history = self._target_history()

        for filename in source_filenames:
            slot_timestamp = self._slot_timestamp_for_source(filename)
            if slot_timestamp is None:
                continue
            if slot_timestamp not in selected_slot_set:
                if len(selected_slots) >= target_history:
                    break
                selected_slots.append(slot_timestamp)
                selected_slot_set.add(slot_timestamp)
            processed = processed_by_slot.get(slot_timestamp)
            if processed is None:
                record = self._load_slot_record(self._raw_dir / f"{slot_timestamp}_glm.json")
                processed = set(str(name) for name in record["source_filenames"])
                processed_by_slot[slot_timestamp] = processed
            if filename in processed:
                continue
            candidate_sources.append(filename)

        latest_slot_output = (
            f"{selected_slots[0]}_glm.json"
            if selected_slots
            else None
        )
        return candidate_sources, latest_slot_output

    def _target_history(self) -> int:
        if any(self._raw_dir.glob("*.json")):
            return self._raw_history
        return min(self._raw_history, BOOTSTRAP_RAW_HISTORY)

    def _kept_raws_on_disk(self) -> list[Path]:
        return sorted(
            self._raw_dir.glob("*.json"),
            key=lambda path: _filename_timestamp_or_min(path.name),
            reverse=True,
        )

    def _listing_prefixes(self) -> list[str]:
        prefixes: list[str] = []
        now = _floor_to_slot(_utc_now() - timedelta(minutes=10))
        seen: set[str] = set()
        for hour_offset in range(LOOKBACK_HOURS):
            hour = now - timedelta(hours=hour_offset)
            prefix = GLM_LISTING_PREFIX_TEMPLATE.format(
                product=GLM_PRODUCT_PREFIX,
                year=hour.strftime("%Y"),
                julian_day=hour.strftime("%j"),
                hour=hour.strftime("%H"),
            )
            if prefix in seen:
                continue
            seen.add(prefix)
            prefixes.append(prefix)
        return prefixes

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


def _datetime_to_slot_timestamp(moment: datetime) -> str:
    return moment.strftime("%Y%j%H%M")


def parse_goes_slot_prefix(filename: str) -> str | None:
    prefix = Path(filename).name.split("_", 1)[0]
    if len(prefix) < 11 or not prefix[:11].isdigit():
        return None
    return prefix[:11]


def _floor_to_slot(moment: datetime) -> datetime:
    floored_minute = moment.minute - (moment.minute % 10)
    return moment.replace(minute=floored_minute, second=0, microsecond=0)


def _utc_now() -> datetime:
    return datetime.now(UTC)
