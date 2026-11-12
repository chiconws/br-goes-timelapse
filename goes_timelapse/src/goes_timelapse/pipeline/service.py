"""Refresh loop: plan, download, render and publish per municipality."""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime
from pathlib import Path
import shutil
import sqlite3
from time import perf_counter
from typing import Any

from typing_extensions import TypedDict

from goes_timelapse.core.config import Settings
from goes_timelapse.core.logging_utils import get_logger
from goes_timelapse.core.models import AreaCatalogEntry, RenderedArea, TrackedArea
from goes_timelapse.core.state import StateStore
from goes_timelapse.data.catalog import AreaCatalog
from goes_timelapse.data.ibge import IbgeGeometryStore
from goes_timelapse.pipeline.download_status import (
    build_initial_download_status,
    build_source_download_summary,
    download_phase_label,
)
from goes_timelapse.pipeline.downloader import (
    AreaCrop,
    DownloadReport,
    GlmDownloader,
    GoesDownloader,
)
from goes_timelapse.pipeline.raw_cache import (
    all_raw_files,
    available_lightning_points_by_timestamp,
    available_raw_paths_by_source,
    prune_raw_cache,
    raw_files_for_source,
    raw_timestamp_count,
)
from goes_timelapse.pipeline.rendering import (
    AreaRenderer,
    FrameSpec,
    WebpBuilder,
    goes_timestamp_to_datetime,
    write_lovelace_snippet,
)
from goes_timelapse.pipeline.runtime_types import (
    RAW_SOURCE_INFRARED,
    RAW_SOURCE_LABELS,
    RAW_SOURCE_LIGHTNING,
    RAW_SOURCE_VISIBLE,
    ActiveDownloadEntry,
    DownloadSourcePlan,
    RawFileEntry,
    SourceDownloadStatus,
)
from goes_timelapse.pipeline.storage_guard import cache_storage_check
from goes_timelapse.pipeline.timeline_runtime import (
    build_area_timeline_plan_for_area,
    build_download_plan_reason,
    build_frame_specs_from_timeline,
    build_global_target_timestamps,
)
from goes_timelapse.timeline import AreaTimelinePlan

LOGGER = get_logger(__name__)


class DownloadSourceSnapshot(TypedDict):
    """Download state of one source for the UI."""

    source_key: str
    source_label: str
    phase: str
    phase_label: str
    is_relevant: bool
    schedule_reason: str
    attempted_count: int
    completed_count: int
    failed_count: int
    active_count: int
    active_downloads: list[ActiveDownloadEntry]
    files_on_disk: list[RawFileEntry]
    file_count: int
    disk_usage_bytes: int
    summary: str


class DownloadsSnapshot(TypedDict):
    """Download state of every source for the UI."""

    sources: list[DownloadSourceSnapshot]


class GoesTimelapseService:
    """Run the refresh loop and serve the API's operations."""

    def __init__(
        self,
        settings: Settings,
        catalog: AreaCatalog,
        state_store: StateStore,
        *,
        geometry_store: IbgeGeometryStore | None = None,
        start_background_tasks: bool = True,
    ):
        """Wire settings, state, catalog and downloaders."""
        self.settings = settings
        self.catalog = catalog
        self.state_store = state_store
        self.geometry_store = geometry_store or IbgeGeometryStore(
            settings.geometry_cache_dir,
            base_url=settings.ibge_malhas_url,
            timeout_seconds=settings.ibge_request_timeout,
        )
        self._downloaders: dict[str, GoesDownloader | GlmDownloader] = {
            RAW_SOURCE_VISIBLE: GoesDownloader(
                base_url=settings.goes_url,
                raw_dir=settings.raw_dir / RAW_SOURCE_VISIBLE,
                band="C02",
                progress_callback=lambda payload: self._update_raw_download_status(
                    RAW_SOURCE_VISIBLE, payload
                ),
            ),
            RAW_SOURCE_INFRARED: GoesDownloader(
                base_url=settings.goes_url,
                raw_dir=settings.raw_dir / RAW_SOURCE_INFRARED,
                band="C13",
                progress_callback=lambda payload: self._update_raw_download_status(
                    RAW_SOURCE_INFRARED, payload
                ),
            ),
            RAW_SOURCE_LIGHTNING: GlmDownloader(
                base_url=settings.goes_url,
                raw_dir=settings.raw_dir / RAW_SOURCE_LIGHTNING,
                progress_callback=lambda payload: self._update_raw_download_status(
                    RAW_SOURCE_LIGHTNING, payload
                ),
            ),
        }
        self.renderer = AreaRenderer(settings)
        self.webp_builder = WebpBuilder(settings)
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._queued_ids: set[str] = set()
        self._tasks: list[asyncio.Task[None]] = []
        self._refresh_lock = asyncio.Lock()
        self._immediate_refresh_task: asyncio.Task[None] | None = None
        self._start_background_tasks = start_background_tasks
        self._status: dict[str, object] = {
            "last_poll_started_at": None,
            "last_poll_finished_at": None,
            "last_poll_new_downloads": 0,
            "last_poll_error": None,
        }
        self._download_status: dict[str, SourceDownloadStatus] = {
            source_key: build_initial_download_status(source_key)
            for source_key in self._downloaders
        }
        self._centroid_cache: dict[str, tuple[float, float]] = {}
        self._bytes_since_start = dict.fromkeys(self._downloaders, 0)
        self._started_at = perf_counter()

    async def start(self) -> None:
        """Start the background refresh and render tasks."""
        self.settings.ensure_directories()
        LOGGER.info(
            "Storage configured: data_dir=%s state_dir=%s media_dir=%s",
            self.settings.data_dir,
            self.settings.state_dir,
            self.settings.media_dir,
        )
        storage = cache_storage_check(self.settings)
        if storage.is_blocking:
            LOGGER.error("Storage guard active at startup: %s", storage.warning)
        elif storage.warning is not None:
            LOGGER.warning("Storage warning at startup: %s", storage.warning)
        for area_id in self.state_store.tracked_ids():
            if self.catalog.get(area_id) is None:
                LOGGER.info("Removing unsupported tracked area %s", area_id)
                self.state_store.remove_tracked(area_id)
                self._cleanup_area_files(area_id)
                continue
            self._set_area_status(area_id, "queued", last_error=None)
            await self.enqueue(area_id)
        if self._start_background_tasks:
            LOGGER.info("Starting background tasks: poller and worker")
            self._tasks.append(
                asyncio.create_task(self._poll_loop(), name="goes-poller")
            )
            self._tasks.append(
                asyncio.create_task(self._worker_loop(), name="goes-worker")
            )

    async def stop(self) -> None:
        """Cancel the background tasks and close sessions."""
        if self._immediate_refresh_task is not None:
            self._immediate_refresh_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._immediate_refresh_task
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self.state_store.close()

    async def add_tracked(self, area_id: str) -> TrackedArea:
        """Track a municipality and queue its first render."""
        area = self.catalog.get(area_id)
        if area is None:
            raise KeyError(area_id)
        already_tracked = self.state_store.is_tracked(area_id)
        if (
            not already_tracked
            and self.state_store.count_tracked() >= self.settings.max_tracked
        ):
            raise ValueError(
                f"Limite máximo de municípios acompanhados atingido ({self.settings.max_tracked})"
            )
        self.state_store.upsert_tracked(area, status="queued")
        await self.enqueue(area_id)
        # A new area starts with an empty folder, so its timeline is read right away.
        if not already_tracked:
            if self._start_background_tasks:
                self._schedule_immediate_refresh()
            else:
                await self.refresh_raw_frames()
        tracked = self.state_store.get_tracked(area_id)
        assert tracked is not None
        return tracked

    async def remove_tracked(self, area_id: str) -> None:
        """Stop tracking a municipality and delete its files."""
        self.state_store.remove_tracked(area_id)
        self._queued_ids.discard(area_id)
        self._cleanup_area_files(area_id)

    async def set_marker(
        self,
        area_id: str,
        *,
        marker_lat: float,
        marker_lon: float,
    ) -> TrackedArea:
        """Set a municipality's marker and re-render it."""
        tracked = self.state_store.get_tracked(area_id)
        if tracked is None:
            raise KeyError(area_id)

        area = self.catalog.get(area_id)
        if area is None:
            raise KeyError(area_id)

        geometry = await asyncio.to_thread(self.geometry_store.load_geometry, area)
        min_lon, min_lat, max_lon, max_lat = self.renderer.destination_bounds(geometry)
        if not (min_lon <= marker_lon <= max_lon and min_lat <= marker_lat <= max_lat):
            raise ValueError("As coordenadas precisam estar dentro da área da imagem")

        self.state_store.set_marker(
            area_id,
            marker_lat=marker_lat,
            marker_lon=marker_lon,
        )
        self._set_area_status(area_id, "queued", last_error=None)
        await self.enqueue(area_id)
        updated = self.state_store.get_tracked(area_id)
        assert updated is not None
        return updated

    async def clear_marker(self, area_id: str) -> TrackedArea:
        """Remove a municipality's marker and re-render it."""
        tracked = self.state_store.get_tracked(area_id)
        if tracked is None:
            raise KeyError(area_id)

        self.state_store.set_marker(
            area_id,
            marker_lat=None,
            marker_lon=None,
        )
        self._set_area_status(area_id, "queued", last_error=None)
        await self.enqueue(area_id)
        updated = self.state_store.get_tracked(area_id)
        assert updated is not None
        return updated

    async def enqueue(self, area_id: str) -> None:
        """Queue a municipality for rendering."""
        if not self.state_store.is_tracked(area_id):
            return
        if area_id in self._queued_ids:
            return
        self._queued_ids.add(area_id)
        await self._queue.put(area_id)

    def tracked(self) -> list[TrackedArea]:
        """Return the tracked municipalities."""
        return self.state_store.list_tracked()

    def search(self, query: str) -> list[AreaCatalogEntry]:
        """Search the municipality catalog."""
        return self.catalog.search(query)

    def snippet_text(self, area_id: str) -> str:
        """Return the Lovelace snippet for a municipality."""
        snippet_path = self.settings.snippets_dir / f"{area_id}.yaml"
        if not snippet_path.exists():
            raise FileNotFoundError(area_id)
        return snippet_path.read_text(encoding="utf-8")

    def media_path(self, area_id: str) -> Path:
        """Return the published WebP path for a municipality."""
        return self.settings.media_dir / f"{area_id}.webp"

    def status_snapshot(self) -> dict[str, object]:
        """Return the service status shown in the UI."""
        files = all_raw_files(self.settings)
        storage = cache_storage_check(self.settings)
        summaries = [
            source["summary"]
            for source in self.downloads_snapshot()["sources"]
            if source["source_key"] != RAW_SOURCE_LIGHTNING
            and (source["is_relevant"] or source["phase"] == "downloading")
        ]
        return {
            **self._status,
            "tracked_count": self.state_store.count_tracked(),
            "queue_length": self._queue.qsize(),
            "raw_frame_count": len(files),
            "raw_timestamp_count": raw_timestamp_count(files),
            "raw_frame_latest": files[0]["label"] if files else None,
            "raw_download_summary": " | ".join(summaries)
            if summaries
            else "Nenhuma fonte ativa",
            "raw_disk_usage_bytes": sum(item["size_bytes"] for item in files),
            "disk_free_bytes": storage.free_bytes,
            "disk_warning": storage.warning,
        }

    def downloads_snapshot(self) -> DownloadsSnapshot:
        """Return the download status shown in the UI."""
        sources: list[DownloadSourceSnapshot] = []
        for source_key in (
            RAW_SOURCE_VISIBLE,
            RAW_SOURCE_INFRARED,
            RAW_SOURCE_LIGHTNING,
        ):
            status = self._download_status[source_key]
            files = self._raw_files_for_source(source_key)
            sources.append(
                {
                    "source_key": source_key,
                    "source_label": RAW_SOURCE_LABELS[source_key],
                    "phase": status["phase"],
                    "phase_label": download_phase_label(str(status["phase"])),
                    "is_relevant": bool(status["is_relevant"]),
                    "schedule_reason": status["schedule_reason"],
                    "attempted_count": status["attempted_count"],
                    "completed_count": status["completed_count"],
                    "failed_count": status["failed_count"],
                    "active_count": status["active_count"],
                    "active_downloads": status["active_downloads"],
                    "files_on_disk": files,
                    "file_count": len(files),
                    "disk_usage_bytes": sum(item["size_bytes"] for item in files),
                    "summary": build_source_download_summary(
                        status, raw_frame_count=len(files)
                    ),
                }
            )
        return {"sources": sources}

    async def _poll_loop(self) -> None:
        poll_seconds = self.settings.poll_minutes * 60
        while True:
            try:
                LOGGER.info("Starting raw refresh cycle")
                await self.refresh_raw_frames()
            except Exception as err:  # pragma: no cover
                LOGGER.exception("GOES refresh failed")
                self._status["last_poll_error"] = str(err)
            await asyncio.sleep(poll_seconds)

    async def refresh_raw_frames(self) -> None:
        """Download the missing raw frames of every source."""
        async with self._refresh_lock:
            self._status["last_poll_started_at"] = _utc_now()
            self._status["last_poll_error"] = None
            self._status["last_poll_new_downloads"] = 0
            plans = await self._build_download_plans()
            LOGGER.info(
                "Raw refresh plans: %s",
                "; ".join(
                    (
                        f"{plan.source_key}: should_download={plan.should_download}, "
                        f"reason={plan.reason}"
                    )
                    for plan in plans
                ),
            )
            for plan in plans:
                self._apply_download_plan(plan)

            storage = cache_storage_check(self.settings)
            LOGGER.trace("Refresh storage check: %s", storage)
            blocking_storage_message = storage.blocking_message
            if blocking_storage_message is not None:
                LOGGER.error("Skipping raw refresh: %s", blocking_storage_message)
                self._status["last_poll_finished_at"] = _utc_now()
                self._status["last_poll_error"] = blocking_storage_message
                self._apply_storage_pause(blocking_storage_message)
                return

            error_messages: list[str] = []
            total_downloads = 0

            try:
                for plan in plans:
                    if not plan.should_download:
                        continue

                    report: DownloadReport | None = None
                    try:
                        LOGGER.trace(
                            "Refreshing source %s with target timestamps: %s",
                            plan.source_key,
                            plan.target_timestamps,
                        )
                        report = await self._refresh_source(plan)
                        assert report is not None
                    except Exception as err:
                        LOGGER.exception("Raw refresh failed for %s", plan.source_key)
                        error_messages.append(
                            f"{RAW_SOURCE_LABELS[plan.source_key]}: {err}"
                        )
                        if self._raw_files_for_source(plan.source_key):
                            self._mark_source_partial_due_to_error(plan.source_key)
                        else:
                            self._mark_source_error(plan.source_key, str(err))
                        continue

                    self._finalize_download_status(plan.source_key, report)
                    LOGGER.info(
                        "Download report for %s: downloaded=%s attempted=%s failed=%s kept=%s",
                        plan.source_key,
                        report.downloaded_count,
                        report.attempted_count,
                        report.failed_count,
                        len(report.kept_files),
                    )
                    self._bytes_since_start[plan.source_key] += report.bytes_read
                    uptime_hours = max((perf_counter() - self._started_at) / 3600, 1e-9)
                    LOGGER.info(
                        "METRIC cycle source=%s files=%s failed=%s bytes=%s requests=%s retries=%s "
                        "listing_s=%.2f elapsed_s=%.2f total_since_start_bytes=%s "
                        "projected_gb_per_day=%.2f",
                        plan.source_key,
                        report.downloaded_count,
                        report.failed_count,
                        report.bytes_read,
                        report.requests,
                        report.retries,
                        report.listing_seconds,
                        report.elapsed_seconds,
                        self._bytes_since_start[plan.source_key],
                        self._bytes_since_start[plan.source_key]
                        / 1e9
                        / uptime_hours
                        * 24,
                    )
                    total_downloads += report.downloaded_count
                    if report.failed_count and not report.kept_files:
                        error_messages.append(
                            f"{RAW_SOURCE_LABELS[plan.source_key]}: falha em {report.failed_count} arquivo(s)"
                        )
                    elif report.failed_count:
                        error_messages.append(
                            f"{RAW_SOURCE_LABELS[plan.source_key]}: {report.failed_count} arquivo(s) falharam"
                        )
            finally:
                self._status["last_poll_finished_at"] = _utc_now()

            latest_retained_timestamp = self._prune_raw_cache()
            self._status["last_poll_new_downloads"] = total_downloads
            if error_messages:
                self._status["last_poll_error"] = " | ".join(error_messages)

            for tracked in self.state_store.list_tracked():
                needs_reprocessing = self._area_needs_reprocessing(
                    tracked.area_id,
                    latest_kept_timestamp=latest_retained_timestamp,
                    downloaded_count=total_downloads,
                )
                LOGGER.trace(
                    "Area reprocessing decision for %s: needs_reprocessing=%s latest_kept=%s downloaded_count=%s",
                    tracked.area_id,
                    needs_reprocessing,
                    latest_retained_timestamp,
                    total_downloads,
                )
                if not needs_reprocessing:
                    continue
                self._set_area_status(tracked.area_id, "queued", last_error=None)
                await self.enqueue(tracked.area_id)

    async def _refresh_source(self, plan: DownloadSourcePlan) -> DownloadReport:
        downloader = self._downloaders[plan.source_key]
        if isinstance(downloader, GlmDownloader):
            return await downloader.refresh_latest(
                target_timestamps=plan.target_timestamps
            )
        return await downloader.refresh_latest(
            target_timestamps=plan.target_timestamps,
            areas=self._area_crops(plan.source_key),
        )

    def _area_crops(self, source_key: str) -> tuple[AreaCrop, ...]:
        return tuple(
            AreaCrop(
                area_id=area.area_id,
                bounds=self.renderer.destination_bounds(
                    self.geometry_store.load_geometry(area)
                ),
                target_timestamps=tuple(
                    frame.slot_timestamp
                    for frame in self._build_area_timeline_plan(area).frames
                    if source_key in frame.required_sources
                ),
            )
            for area in self._tracked_catalog_areas()
        )

    def _set_area_status(
        self,
        area_id: str,
        status: str,
        **kwargs: Any,
    ) -> bool:
        try:
            self.state_store.set_status(area_id, status, **kwargs)
        except sqlite3.Error:
            LOGGER.exception(
                "Failed to persist area status for %s -> %s",
                area_id,
                status,
            )
            return False
        return True

    def _schedule_immediate_refresh(self) -> None:
        if (
            self._immediate_refresh_task is not None
            and not self._immediate_refresh_task.done()
        ):
            return
        self._immediate_refresh_task = asyncio.create_task(
            self._run_immediate_refresh(),
            name="goes-immediate-refresh",
        )

    async def _run_immediate_refresh(self) -> None:
        try:
            await self.refresh_raw_frames()
        except Exception:  # pragma: no cover
            LOGGER.exception("Immediate GOES refresh failed")

    async def _worker_loop(self) -> None:
        while True:
            area_id = await self._queue.get()
            self._queued_ids.discard(area_id)
            try:
                if self.state_store.is_tracked(area_id):
                    await asyncio.to_thread(self._process_area, area_id)
            except Exception:  # pragma: no cover
                LOGGER.exception("Area processing crashed for %s", area_id)
                self._set_area_status(
                    area_id,
                    "error",
                    last_error="Falha inesperada no processamento",
                )
            finally:
                self._queue.task_done()

    def _process_area(self, area_id: str) -> RenderedArea | None:
        area = self.catalog.get(area_id)
        if area is None:
            self._set_area_status(area_id, "error", last_error="Área não encontrada")
            return None
        tracked = self.state_store.get_tracked(area_id)
        if tracked is None:
            return None

        frame_specs = self._build_frame_specs(area)
        LOGGER.trace(
            "Area %s resolved %s frame spec(s): %s",
            area_id,
            len(frame_specs),
            [frame.timestamp for frame in frame_specs],
        )
        if not frame_specs:
            if self.media_path(area_id).exists():
                self._set_area_status(area_id, "ready", last_error=None)
            else:
                self._set_area_status(area_id, "queued", last_error=None)
            return None

        self._set_area_status(area_id, "processing", last_error=None)
        try:
            geometry = self.geometry_store.load_geometry(area)
            marker_coordinates = None
            if tracked.marker_lat is not None and tracked.marker_lon is not None:
                marker_coordinates = (tracked.marker_lon, tracked.marker_lat)
            png_paths = self.renderer.process_frames(
                area,
                geometry,
                frame_specs,
                marker_coordinates=marker_coordinates,
            )
            LOGGER.trace(
                "Area %s rendered %s PNG frame(s): %s",
                area_id,
                len(png_paths),
                [path.name for path in png_paths],
            )
            if not png_paths:
                self._set_area_status(
                    area_id, "error", last_error="Nenhum quadro foi renderizado"
                )
                return None

            media_path = self.webp_builder.build(area_id, png_paths)
            snippet_path = write_lovelace_snippet(
                self.settings.snippets_dir,
                area,
                area_id,
            )
            latest_source_timestamp = frame_specs[-1].timestamp
            latest_moment = goes_timestamp_to_datetime(latest_source_timestamp)
            LOGGER.info(
                "METRIC area_ready area=%s frames=%s latest_slot=%s slot_age_s=%.0f",
                area_id,
                len(png_paths),
                latest_source_timestamp,
                (datetime.now(UTC) - latest_moment).total_seconds()
                if latest_moment
                else -1,
            )

            if not self.state_store.is_tracked(area_id):
                self._cleanup_area_files(area_id)
                return None

            self._set_area_status(
                area_id,
                "ready",
                last_error=None,
                latest_source_timestamp=latest_source_timestamp,
                media_path=str(media_path),
                snippet_path=str(snippet_path),
            )
            return RenderedArea(
                area=area,
                png_paths=png_paths,
                media_path=media_path,
                snippet_path=snippet_path,
                latest_source_timestamp=latest_source_timestamp,
            )
        except Exception as err:
            LOGGER.exception("Failed to process %s", area_id)
            self._set_area_status(area_id, "error", last_error=str(err))
            return None

    def _cleanup_area_files(self, area_id: str) -> None:
        self.renderer.cleanup(area_id)
        for source_key in (RAW_SOURCE_VISIBLE, RAW_SOURCE_INFRARED):
            shutil.rmtree(
                self.settings.raw_dir / source_key / area_id, ignore_errors=True
            )
        self.media_path(area_id).unlink(missing_ok=True)
        (self.settings.snippets_dir / f"{area_id}.yaml").unlink(missing_ok=True)

    @staticmethod
    def _is_valid_raw(path: Path) -> bool:
        try:
            return path.is_file() and path.stat().st_size > 0
        except OSError:
            LOGGER.warning("Skipping unreadable raw frame metadata: %s", path)
            return False

    def _update_raw_download_status(
        self, source_key: str, payload: dict[str, object]
    ) -> None:
        self._download_status[source_key].update(payload)  # type: ignore[typeddict-item]

    def _finalize_download_status(
        self, source_key: str, report: DownloadReport
    ) -> None:
        status = self._download_status[source_key]
        if report.failed_count and report.kept_files:
            phase = "partial"
        elif report.failed_count:
            phase = "error"
        elif report.kept_files:
            phase = "ready"
        else:
            phase = "idle"

        status.update(
            {
                "phase": phase,
                "attempted_count": report.attempted_count,
                "completed_count": report.attempted_count,
                "failed_count": report.failed_count,
                "active_count": 0,
                "active_downloads": [],
            }
        )

    def _mark_source_error(self, source_key: str, message: str) -> None:
        status = self._download_status[source_key]
        status.update(
            {
                "phase": "error",
                "active_count": 0,
                "active_downloads": [],
                "schedule_reason": message,
            }
        )

    def _mark_source_partial_due_to_error(self, source_key: str) -> None:
        status = self._download_status[source_key]
        status.update(
            {
                "phase": "partial",
                "active_count": 0,
                "active_downloads": [],
                "schedule_reason": "Usando o cache local; a última atualização da NOAA falhou",
            }
        )

    def _apply_download_plan(self, plan: DownloadSourcePlan) -> None:
        status = self._download_status[plan.source_key]
        LOGGER.trace(
            "Applying download plan for %s: should_download=%s tracked_areas=%s targets=%s reason=%s",
            plan.source_key,
            plan.should_download,
            plan.tracked_area_ids,
            plan.target_timestamps,
            plan.reason,
        )
        status["is_relevant"] = bool(plan.tracked_area_ids)
        status["schedule_reason"] = plan.reason
        if plan.should_download:
            if status["phase"] in {"disabled", "paused"}:
                status["phase"] = "idle"
            return
        status["phase"] = "paused" if plan.tracked_area_ids else "disabled"
        status["active_count"] = 0
        status["active_downloads"] = []
        status["attempted_count"] = 0
        status["completed_count"] = 0
        status["failed_count"] = 0

    def _apply_storage_pause(self, reason: str) -> None:
        for status in self._download_status.values():
            status["active_count"] = 0
            status["active_downloads"] = []
            status["attempted_count"] = 0
            status["completed_count"] = 0
            status["failed_count"] = 0
            if status.get("is_relevant"):
                status["phase"] = "paused"
                status["schedule_reason"] = reason

    async def _build_download_plans(self) -> list[DownloadSourcePlan]:
        tracked_areas = self._tracked_catalog_areas()
        tracked_area_ids = tuple(area.area_id for area in tracked_areas)
        LOGGER.trace(
            "Tracked areas considered for download planning: %s",
            [
                {
                    "area_id": area.area_id,
                    "display_name": area.display_name,
                }
                for area in tracked_areas
            ],
        )

        if not tracked_area_ids:
            return [
                DownloadSourcePlan(
                    source_key=source_key,
                    source_label=RAW_SOURCE_LABELS[source_key],
                    tracked_area_ids=(),
                    should_download=False,
                    target_timestamps=(),
                    reason="Nenhum município acompanhado",
                )
                for source_key in (
                    RAW_SOURCE_VISIBLE,
                    RAW_SOURCE_INFRARED,
                    RAW_SOURCE_LIGHTNING,
                )
            ]

        target_timestamps_by_source = self._build_global_target_timestamps(
            tracked_areas
        )
        LOGGER.trace(
            "Download planning target timestamps by source: %s",
            target_timestamps_by_source,
        )
        return [
            DownloadSourcePlan(
                source_key=source_key,
                source_label=RAW_SOURCE_LABELS[source_key],
                tracked_area_ids=tracked_area_ids,
                should_download=bool(target_timestamps_by_source[source_key]),
                target_timestamps=target_timestamps_by_source[source_key],
                reason=build_download_plan_reason(
                    source_key=source_key,
                    tracked_area_ids=tracked_area_ids,
                    target_timestamps=target_timestamps_by_source[source_key],
                ),
            )
            for source_key in (
                RAW_SOURCE_VISIBLE,
                RAW_SOURCE_INFRARED,
                RAW_SOURCE_LIGHTNING,
            )
        ]

    def _tracked_catalog_areas(self) -> list[AreaCatalogEntry]:
        return [
            area
            for tracked in self.state_store.list_tracked()
            if (area := self.catalog.get(tracked.area_id)) is not None
        ]

    def _build_area_timeline_plan(
        self,
        area: AreaCatalogEntry,
        *,
        reference_moment: datetime | None = None,
    ) -> AreaTimelinePlan:
        return build_area_timeline_plan_for_area(
            area=area,
            settings=self.settings,
            resolve_area_centroid=self._resolve_area_centroid,
            reference_moment=reference_moment,
        )

    def _build_global_target_timestamps(
        self,
        tracked_areas: list[AreaCatalogEntry],
        *,
        reference_moment: datetime | None = None,
    ) -> dict[str, tuple[str, ...]]:
        return build_global_target_timestamps(
            tracked_areas=tracked_areas,
            build_area_timeline=lambda area, moment: self._build_area_timeline_plan(
                area,
                reference_moment=moment,
            ),
            reference_moment=reference_moment,
        )

    def _resolve_area_centroid(self, area: AreaCatalogEntry) -> tuple[float, float]:
        cached = self._centroid_cache.get(area.area_id)
        if cached is not None:
            return cached
        geometry = self.geometry_store.load_geometry(area)
        self._centroid_cache[area.area_id] = geometry.centroid
        return geometry.centroid

    def _build_frame_specs(self, area: AreaCatalogEntry) -> list[FrameSpec]:
        return build_frame_specs_from_timeline(
            area=area,
            build_area_timeline=lambda area_obj: self._build_area_timeline_plan(
                area_obj
            ),
            source_paths=self._available_raw_paths_by_source(area.area_id),
            lightning_points_by_timestamp=available_lightning_points_by_timestamp(
                self.settings
            ),
        )

    def _area_needs_reprocessing(
        self,
        area_id: str,
        *,
        latest_kept_timestamp: str | None,
        downloaded_count: int,
    ) -> bool:
        if downloaded_count > 0:
            return True

        tracked = self.state_store.get_tracked(area_id)
        if tracked is None:
            return False

        if tracked.status in {"queued", "processing", "error"}:
            return True

        if latest_kept_timestamp is None:
            return False

        if tracked.latest_source_timestamp is None:
            return True

        return tracked.latest_source_timestamp < latest_kept_timestamp

    def _raw_files_for_source(self, source_key: str) -> list[RawFileEntry]:
        return raw_files_for_source(self.settings, source_key)

    def _available_raw_paths_by_source(
        self, area_id: str
    ) -> dict[str, dict[str, Path]]:
        return available_raw_paths_by_source(self.settings, area_id, self._is_valid_raw)

    def _build_global_keep_timestamps(self) -> dict[str, tuple[str, ...]]:
        return self._build_global_target_timestamps(self._tracked_catalog_areas())

    def _prune_raw_cache(self) -> str | None:
        return prune_raw_cache(
            settings=self.settings,
            keep_timestamps_by_source=self._build_global_keep_timestamps(),
        )


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()
