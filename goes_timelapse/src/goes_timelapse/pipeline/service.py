from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
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
from goes_timelapse.pipeline.downloader import DownloadReport, GlmDownloader, GoesDownloader
from goes_timelapse.pipeline.marker_geometry import point_within_polygon
from goes_timelapse.pipeline.raw_cache import (
    all_raw_files,
    available_lightning_points_by_timestamp,
    available_raw_paths_by_source,
    prune_raw_cache,
    raw_disk_usage_bytes,
    raw_files_for_source,
    raw_timestamp_count,
)
from goes_timelapse.pipeline.rendering import (
    AreaRenderer,
    FrameSpec,
    WebpBuilder,
    write_lovelace_snippet,
)
from goes_timelapse.pipeline.runtime_types import (
    ActiveDownloadEntry,
    RAW_SOURCE_INFRARED,
    RAW_SOURCE_LABELS,
    RAW_SOURCE_LIGHTNING,
    RAW_SOURCE_VISIBLE,
    DownloadSourcePlan,
    RawFileEntry,
    SourceDownloadStatus,
    StorageCheck,
)
from goes_timelapse.pipeline.storage_guard import (
    blocking_storage_message,
    build_storage_checks,
    format_bytes,
    storage_warning_summary,
    worst_staging_check,
)
from goes_timelapse.pipeline.timeline_runtime import (
    build_area_timeline_plan_for_area,
    build_download_plan_reason,
    build_frame_specs_from_timeline,
    build_global_target_timestamps,
)
from goes_timelapse.timeline import AreaTimelinePlan


LOGGER = get_logger(__name__)


class DownloadSourceSnapshot(TypedDict):
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
    current_file: str | None
    last_downloaded: str | None
    latest_available: str | None
    active_downloads: list[ActiveDownloadEntry]
    files_on_disk: list[RawFileEntry]
    file_count: int
    disk_usage_bytes: int
    summary: str


class DownloadsSnapshot(TypedDict):
    sources: list[DownloadSourceSnapshot]

class GoesTimelapseService:
    def __init__(
        self,
        settings: Settings,
        catalog: AreaCatalog,
        state_store: StateStore,
        *,
        geometry_store: IbgeGeometryStore | None = None,
        start_background_tasks: bool = True,
    ):
        self.settings = settings
        self.catalog = catalog
        self.state_store = state_store
        self.geometry_store = geometry_store or IbgeGeometryStore(
            settings.geometry_cache_dir,
            base_url=settings.ibge_malhas_url,
            timeout_seconds=settings.ibge_request_timeout,
        )
        self._downloaders = {
            RAW_SOURCE_VISIBLE: GoesDownloader(
                base_url=settings.goes_url,
                source_dir=settings.source_dir / RAW_SOURCE_VISIBLE,
                raw_dir=settings.raw_dir / RAW_SOURCE_VISIBLE,
                raw_history=settings.raw_history,
                band="C02",
                scratch_dir=settings.scratch_dir / RAW_SOURCE_VISIBLE,
                progress_callback=lambda payload: self._update_raw_download_status(
                    RAW_SOURCE_VISIBLE, payload
                ),
            ),
            RAW_SOURCE_INFRARED: GoesDownloader(
                base_url=settings.goes_url,
                source_dir=settings.source_dir / RAW_SOURCE_INFRARED,
                raw_dir=settings.raw_dir / RAW_SOURCE_INFRARED,
                raw_history=settings.raw_history,
                band="C13",
                scratch_dir=settings.scratch_dir / RAW_SOURCE_INFRARED,
                progress_callback=lambda payload: self._update_raw_download_status(
                    RAW_SOURCE_INFRARED, payload
                ),
            ),
            RAW_SOURCE_LIGHTNING: GlmDownloader(
                base_url=settings.goes_url,
                source_dir=settings.source_dir / RAW_SOURCE_LIGHTNING,
                raw_dir=settings.raw_dir / RAW_SOURCE_LIGHTNING,
                raw_history=settings.raw_history,
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
            source_key: self._initial_download_status(source_key)
            for source_key in self._downloaders
        }
        self._centroid_cache: dict[str, tuple[float, float]] = {}

    async def start(self) -> None:
        self.settings.ensure_directories()
        self.settings.configure_runtime_environment()
        LOGGER.info(
            "Storage configured: data_dir=%s state_dir=%s source_dir=%s scratch_dir=%s media_dir=%s",
            self.settings.data_dir,
            self.settings.state_dir,
            self.settings.source_dir,
            self.settings.scratch_dir,
            self.settings.media_dir,
        )
        if self.settings.source_dir_warning:
            LOGGER.warning(self.settings.source_dir_warning)
        if self.settings.scratch_dir_warning:
            LOGGER.warning(self.settings.scratch_dir_warning)
        storage_checks = self._storage_checks()
        LOGGER.trace("Startup storage checks: %s", storage_checks)
        blocking_storage_message = self._blocking_storage_message(storage_checks)
        if blocking_storage_message is not None:
            LOGGER.error("Storage guard active at startup: %s", blocking_storage_message)
        else:
            storage_warning = self._storage_warning_summary(storage_checks)
            if storage_warning is not None:
                LOGGER.warning("Storage warning at startup: %s", storage_warning)
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
            self._tasks.append(asyncio.create_task(self._poll_loop(), name="goes-poller"))
            self._tasks.append(asyncio.create_task(self._worker_loop(), name="goes-worker"))

    async def stop(self) -> None:
        if self._immediate_refresh_task is not None:
            self._immediate_refresh_task.cancel()
            try:
                await self._immediate_refresh_task
            except asyncio.CancelledError:
                pass
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self.state_store.close()

    async def add_tracked(self, area_id: str) -> TrackedArea:
        area = self.catalog.get(area_id)
        if area is None:
            raise KeyError(area_id)
        already_tracked = self.state_store.is_tracked(area_id)
        tracked_before = self.state_store.count_tracked()
        if (
            not already_tracked
            and self.state_store.count_tracked() >= self.settings.max_tracked
        ):
            raise ValueError(
                f"Limite máximo de municípios acompanhados atingido ({self.settings.max_tracked})"
            )
        self.state_store.upsert_tracked(area, status="queued")
        await self.enqueue(area_id)
        if not already_tracked and tracked_before == 0:
            if self._start_background_tasks:
                self._schedule_immediate_refresh()
            else:
                await self.refresh_raw_frames()
        tracked = self.state_store.get_tracked(area_id)
        assert tracked is not None
        return tracked

    async def remove_tracked(self, area_id: str) -> None:
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
        tracked = self.state_store.get_tracked(area_id)
        if tracked is None:
            raise KeyError(area_id)

        area = self.catalog.get(area_id)
        if area is None or area.area_type != "municipio":
            raise ValueError("O marcador só é suportado para municípios acompanhados")

        geometry = await asyncio.to_thread(self.geometry_store.load_geometry, area)
        if not point_within_polygon((marker_lon, marker_lat), geometry.polygon):
            raise ValueError("As coordenadas precisam estar dentro do município selecionado")

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
        if not self.state_store.is_tracked(area_id):
            return
        if area_id in self._queued_ids:
            return
        self._queued_ids.add(area_id)
        await self._queue.put(area_id)

    def tracked(self) -> list[TrackedArea]:
        return self.state_store.list_tracked()

    def search(self, query: str) -> list[AreaCatalogEntry]:
        return self.catalog.search(query)

    def snippet_text(self, area_id: str) -> str:
        snippet_path = self.settings.snippets_dir / f"{area_id}.yaml"
        if not snippet_path.exists():
            raise FileNotFoundError(area_id)
        return snippet_path.read_text(encoding="utf-8")

    def media_path(self, area_id: str) -> Path:
        return self.settings.media_dir / f"{area_id}.webp"

    def status_snapshot(self) -> dict[str, object]:
        files = self._all_raw_files()
        raw_disk_bytes = self._raw_disk_usage_bytes()
        storage_checks = self._storage_checks()
        cache_check = storage_checks["cache"]
        staging_check = self._worst_staging_check(storage_checks)
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
            "raw_timestamp_count": self._raw_timestamp_count(),
            "raw_history_limit": self.settings.raw_history,
            "raw_frame_latest": files[0]["label"] if files else None,
            "raw_download_summary": " | ".join(summaries) if summaries else "Nenhuma fonte ativa",
            "raw_disk_usage_bytes": raw_disk_bytes,
            "disk_free_bytes": cache_check.free_bytes,
            "disk_total_bytes": cache_check.total_bytes,
            "staging_free_bytes": staging_check.free_bytes if staging_check else None,
            "staging_total_bytes": staging_check.total_bytes if staging_check else None,
            "staging_path": str(staging_check.path) if staging_check else None,
            "staging_warning": staging_check.warning if staging_check else None,
            "disk_warning": self._storage_warning_summary(storage_checks),
        }

    def downloads_snapshot(self) -> DownloadsSnapshot:
        sources: list[DownloadSourceSnapshot] = []
        for source_key in (RAW_SOURCE_VISIBLE, RAW_SOURCE_INFRARED, RAW_SOURCE_LIGHTNING):
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
                    "current_file": status["current_file"],
                    "last_downloaded": status["last_downloaded"],
                    "latest_available": status["latest_available"],
                    "active_downloads": status["active_downloads"],
                    "files_on_disk": files,
                    "file_count": len(files),
                    "disk_usage_bytes": sum(item["size_bytes"] for item in files),
                    "summary": self._source_download_summary(source_key, len(files)),
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

            storage_checks = self._storage_checks()
            LOGGER.trace("Refresh storage checks: %s", storage_checks)
            blocking_storage_message = self._blocking_storage_message(storage_checks)
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
                        report = await self._downloaders[plan.source_key].refresh_latest(
                            target_timestamps=plan.target_timestamps,
                        )
                        assert report is not None
                    except Exception as err:
                        LOGGER.exception("Raw refresh failed for %s", plan.source_key)
                        if self._raw_files_for_source(plan.source_key):
                            self._mark_source_partial_due_to_error(plan.source_key)
                        else:
                            self._mark_source_error(plan.source_key, str(err))
                            error_messages.append(
                                f"{RAW_SOURCE_LABELS[plan.source_key]}: {err}"
                            )
                        continue

                    self._finalize_download_status(plan.source_key, report)
                    LOGGER.info(
                        "Download report for %s: downloaded=%s attempted=%s failed=%s kept=%s latest_available=%s last_downloaded=%s",
                        plan.source_key,
                        report.downloaded_count,
                        report.attempted_count,
                        report.failed_count,
                        [path.name for path in report.kept_files],
                        report.latest_available,
                        report.last_downloaded,
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

    def _set_area_status(
        self,
        area_id: str,
        status: str,
        **kwargs,
    ) -> bool:
        try:
            self.state_store.set_status(area_id, status, **kwargs)
            return True
        except sqlite3.Error as err:
            LOGGER.exception(
                "Failed to persist area status for %s -> %s: %s",
                area_id,
                status,
                err,
            )
            return False

    def _schedule_immediate_refresh(self) -> None:
        if self._immediate_refresh_task is not None and not self._immediate_refresh_task.done():
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
        existing_media_path = self._existing_media_path(area_id, tracked)
        if not frame_specs:
            if existing_media_path is not None:
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
        self.media_path(area_id).unlink(missing_ok=True)
        (self.settings.media_dir / f"{area_id}.gif").unlink(missing_ok=True)
        (self.settings.snippets_dir / f"{area_id}.yaml").unlink(missing_ok=True)

    @staticmethod
    def _is_valid_raw(path: Path) -> bool:
        try:
            return path.is_file() and path.stat().st_size > 0
        except OSError:
            LOGGER.warning("Skipping unreadable raw frame metadata: %s", path)
            return False

    _process_municipality = _process_area

    def _update_raw_download_status(self, source_key: str, payload: dict[str, object]) -> None:
        status = self._download_status[source_key]
        status["phase"] = str(payload.get("phase", "idle"))
        status["attempted_count"] = _coerce_int(payload.get("attempted_count"))
        status["completed_count"] = _coerce_int(payload.get("completed_count"))
        status["failed_count"] = _coerce_int(payload.get("failed_count"))
        status["active_count"] = _coerce_int(payload.get("active_count"))
        current_file = payload.get("current_file")
        latest_available = payload.get("latest_available")
        status["current_file"] = str(current_file) if current_file is not None else None
        status["latest_available"] = (
            str(latest_available) if latest_available is not None else None
        )
        active_downloads = payload.get("active_downloads", [])
        status["active_downloads"] = (
            [
                {
                    "filename": str(item.get("filename", "")),
                    "downloaded_bytes": int(item.get("downloaded_bytes", 0)),
                    "total_bytes": (
                        _coerce_int(item["total_bytes"])
                        if item.get("total_bytes") is not None
                        else None
                    ),
                    "percent": (
                        float(item["percent"]) if item.get("percent") is not None else None
                    ),
                    "stage": str(item.get("stage", "")),
                }
                for item in active_downloads
                if isinstance(item, dict)
            ]
            if isinstance(active_downloads, list)
            else []
        )
        last_downloaded = payload.get("last_downloaded")
        if last_downloaded:
            status["last_downloaded"] = str(last_downloaded)

    def _finalize_download_status(self, source_key: str, report: DownloadReport) -> None:
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
                "current_file": None,
                "last_downloaded": report.last_downloaded or status.get("last_downloaded"),
                "latest_available": report.latest_available,
                "active_downloads": [],
            }
        )

    def _mark_source_error(self, source_key: str, message: str) -> None:
        status = self._download_status[source_key]
        status.update(
            {
                "phase": "error",
                "current_file": None,
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
                "current_file": None,
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
        status["current_file"] = None
        status["active_count"] = 0
        status["active_downloads"] = []
        status["attempted_count"] = 0
        status["completed_count"] = 0
        status["failed_count"] = 0

    def _apply_storage_pause(self, reason: str) -> None:
        for status in self._download_status.values():
            status["current_file"] = None
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

        target_timestamps_by_source = self._build_global_target_timestamps(tracked_areas)
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
                reason=self._download_plan_reason(
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

    def _download_plan_reason(
        self,
        *,
        source_key: str,
        tracked_area_ids: tuple[str, ...],
        target_timestamps: tuple[str, ...],
    ) -> str:
        return build_download_plan_reason(
            source_key=source_key,
            tracked_area_ids=tracked_area_ids,
            target_timestamps=target_timestamps,
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
            build_area_timeline=lambda area_obj: self._build_area_timeline_plan(area_obj),
            source_paths=self._available_raw_paths_by_source(),
            lightning_points_by_timestamp=self._available_lightning_points_by_timestamp(),
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

    def _source_download_summary(self, source_key: str, raw_frame_count: int) -> str:
        return build_source_download_summary(
            self._download_status[source_key],
            raw_frame_count=raw_frame_count,
        )

    def _initial_download_status(self, source_key: str) -> SourceDownloadStatus:
        return build_initial_download_status(source_key)

    def _all_raw_files(self) -> list[RawFileEntry]:
        return all_raw_files(
            self.settings,
            tuple(self._downloaders.keys()),
        )

    def _raw_timestamp_count(self) -> int:
        return raw_timestamp_count(self._all_raw_files())

    def _raw_disk_usage_bytes(self) -> int:
        return raw_disk_usage_bytes(self.settings, tuple(self._downloaders.keys()))

    def _raw_files_for_source(self, source_key: str) -> list[RawFileEntry]:
        return raw_files_for_source(self.settings, source_key)

    def _available_raw_paths_by_source(self) -> dict[str, dict[str, Path]]:
        return available_raw_paths_by_source(self.settings, self._is_valid_raw)

    def _available_lightning_points_by_timestamp(
        self,
    ) -> dict[str, tuple[tuple[float, float], ...]]:
        return available_lightning_points_by_timestamp(self.settings, LOGGER)

    def _build_global_keep_timestamps(self) -> dict[str, tuple[str, ...]]:
        tracked_areas = self._tracked_catalog_areas()
        if not tracked_areas:
            return {
                RAW_SOURCE_VISIBLE: (),
                RAW_SOURCE_INFRARED: (),
                RAW_SOURCE_LIGHTNING: (),
            }
        return self._build_global_target_timestamps(tracked_areas)

    def _prune_raw_cache(self) -> str | None:
        return prune_raw_cache(
            settings=self.settings,
            downloaders=self._downloaders,
            source_keys=tuple(self._downloaders.keys()),
            keep_timestamps_by_source=self._build_global_keep_timestamps(),
            logger=LOGGER,
        )

    def _existing_media_path(
        self,
        area_id: str,
        tracked: TrackedArea,
    ) -> Path | None:
        candidates: list[Path] = []
        if tracked.media_path:
            candidates.append(Path(tracked.media_path))
        candidates.append(self.media_path(area_id))
        for path in candidates:
            if path.exists():
                return path
        return None

    def _storage_checks(self) -> dict[str, StorageCheck]:
        return build_storage_checks(self.settings)

    @staticmethod
    def _worst_staging_check(
        storage_checks: dict[str, StorageCheck],
    ) -> StorageCheck | None:
        return worst_staging_check(storage_checks)

    @classmethod
    def _storage_warning_summary(
        cls,
        storage_checks: dict[str, StorageCheck],
    ) -> str | None:
        return storage_warning_summary(storage_checks)

    @classmethod
    def _blocking_storage_message(
        cls,
        storage_checks: dict[str, StorageCheck],
    ) -> str | None:
        return blocking_storage_message(storage_checks)

    @staticmethod
    def _format_bytes(value: int) -> str:
        return format_bytes(value)


def _coerce_int(value: object | None) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()
