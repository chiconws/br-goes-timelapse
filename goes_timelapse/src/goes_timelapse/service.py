from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from rasterio.windows import Window

from goes_timelapse.catalog import AreaCatalog
from goes_timelapse.config import Settings
from goes_timelapse.downloader import DownloadReport, GlmDownloader, GoesDownloader
from goes_timelapse.ibge import IbgeGeometryStore
from goes_timelapse.models import AreaCatalogEntry, RenderedArea, TrackedArea
from goes_timelapse.raster_sources import open_raster_source
from goes_timelapse.rendering import (
    AreaRenderer,
    FrameSpec,
    WebpBuilder,
    parse_goes_timestamp,
    write_lovelace_snippet,
)
from goes_timelapse.solar import (
    DEFAULT_TRANSITION_BLEND_WEIGHTS,
    is_within_visible_window,
    sunrise_transition_alpha,
    sunset_transition_alpha,
)
from goes_timelapse.state import StateStore
from goes_timelapse.timeline import (
    PHASE_INFRARED,
    PHASE_VISIBLE,
    PHASE_SUNRISE_BLEND,
    PHASE_SUNSET_BLEND,
    SOURCE_INFRARED,
    SOURCE_LIGHTNING,
    SOURCE_VISIBLE,
    AreaTimelinePlan,
    TimelineFrame,
    datetime_to_slot_timestamp,
    floor_to_slot,
)


LOGGER = logging.getLogger(__name__)

RAW_SOURCE_VISIBLE = "visible"
RAW_SOURCE_INFRARED = "infrared"
RAW_SOURCE_LIGHTNING = "lightning"
RAW_SOURCE_LABELS = {
    RAW_SOURCE_VISIBLE: "Visível B2",
    RAW_SOURCE_INFRARED: "Infravermelho B13",
    RAW_SOURCE_LIGHTNING: "Descargas GLM",
}
TRANSITION_BLEND_WEIGHTS = DEFAULT_TRANSITION_BLEND_WEIGHTS
SLOT_MINUTES = 10


@dataclass(slots=True, frozen=True)
class DownloadSourcePlan:
    source_key: str
    source_label: str
    tracked_area_ids: tuple[str, ...]
    should_download: bool
    reason: str
    target_timestamps: tuple[str, ...] = ()


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
        self._download_status = {
            source_key: self._initial_download_status(source_key)
            for source_key in self._downloaders
        }
        self._centroid_cache: dict[str, tuple[float, float]] = {}

    async def start(self) -> None:
        self.settings.ensure_directories()
        self.settings.configure_runtime_environment()
        LOGGER.info(
            "Storage configured: data_dir=%s state_dir=%s scratch_dir=%s media_dir=%s",
            self.settings.data_dir,
            self.settings.state_dir,
            self.settings.scratch_dir,
            self.settings.media_dir,
        )
        if self.settings.scratch_dir_warning:
            LOGGER.warning(self.settings.scratch_dir_warning)
        for area_id in self.state_store.tracked_ids():
            if self.catalog.get(area_id) is None:
                LOGGER.info("Removing unsupported tracked area %s", area_id)
                self.state_store.remove_tracked(area_id)
                self._cleanup_area_files(area_id)
                continue
            self._set_area_status(area_id, "queued", last_error=None)
            await self.enqueue(area_id)
        if self._start_background_tasks:
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
        if not _point_within_polygon((marker_lon, marker_lat), geometry.polygon):
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
        data_usage = shutil.disk_usage(self.settings.data_dir)
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
            "raw_history_limit": self.settings.frame_count,
            "raw_frame_latest": files[0]["label"] if files else None,
            "raw_download_summary": " | ".join(summaries) if summaries else "Nenhuma fonte ativa",
            "raw_disk_usage_bytes": raw_disk_bytes,
            "disk_free_bytes": data_usage.free,
            "disk_total_bytes": data_usage.total,
            "disk_warning": self._disk_warning(data_usage.free),
        }

    def downloads_snapshot(self) -> dict[str, object]:
        sources = []
        for source_key in (RAW_SOURCE_VISIBLE, RAW_SOURCE_INFRARED, RAW_SOURCE_LIGHTNING):
            status = self._download_status[source_key]
            files = self._raw_files_for_source(source_key)
            sources.append(
                {
                    "source_key": source_key,
                    "source_label": RAW_SOURCE_LABELS[source_key],
                    "phase": status["phase"],
                    "phase_label": _download_phase_label(str(status["phase"])),
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
                    "disk_usage_bytes": sum(int(item["size_bytes"]) for item in files),
                    "summary": self._source_download_summary(source_key, len(files)),
                }
            )
        return {"sources": sources}

    async def _poll_loop(self) -> None:
        poll_seconds = self.settings.poll_minutes * 60
        while True:
            try:
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
            reference_moment = self._timeline_reference_moment()
            plans = await self._build_download_plans(reference_moment=reference_moment)
            error_messages: list[str] = []
            total_downloads = 0

            try:
                for plan in plans:
                    self._apply_download_plan(plan)

                for plan in plans:
                    if not plan.should_download:
                        continue

                    report: DownloadReport | None = None
                    try:
                        report = await self._downloaders[plan.source_key].refresh_latest(
                            target_timestamps=plan.target_timestamps
                        )
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

            latest_retained_timestamp = self._prune_raw_cache(
                reference_moment=reference_moment
            )
            self._status["last_poll_new_downloads"] = total_downloads
            if error_messages:
                self._status["last_poll_error"] = " | ".join(error_messages)

            for tracked in self.state_store.list_tracked():
                if not self._area_needs_reprocessing(
                    tracked.area_id,
                    latest_kept_timestamp=latest_retained_timestamp,
                    downloaded_count=total_downloads,
                ):
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
        if not frame_specs:
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
            source = open_raster_source(path)
            try:
                source.read_image(Window(col_off=0, row_off=0, width=1, height=1))
            finally:
                source.close()
            return True
        except Exception:
            LOGGER.warning("Skipping invalid raw frame: %s", path)
            return False

    _process_municipality = _process_area

    def _update_raw_download_status(self, source_key: str, payload: dict[str, object]) -> None:
        status = self._download_status[source_key]
        status["phase"] = payload.get("phase", "idle")
        status["attempted_count"] = payload.get("attempted_count", 0)
        status["completed_count"] = payload.get("completed_count", 0)
        status["failed_count"] = payload.get("failed_count", 0)
        status["active_count"] = payload.get("active_count", 0)
        status["current_file"] = payload.get("current_file")
        status["latest_available"] = payload.get("latest_available")
        status["active_downloads"] = payload.get("active_downloads", [])
        last_downloaded = payload.get("last_downloaded")
        if last_downloaded:
            status["last_downloaded"] = last_downloaded

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

    async def _build_download_plans(
        self,
        *,
        reference_moment: datetime | None = None,
    ) -> list[DownloadSourcePlan]:
        tracked_areas = self._tracked_catalog_areas()
        tracked_area_ids = tuple(area.area_id for area in tracked_areas)
        reference_moment = reference_moment or self._timeline_reference_moment()
        target_timestamps = self._build_global_target_timestamps(
            tracked_areas,
            reference_moment=reference_moment,
        )

        return [
            DownloadSourcePlan(
                source_key=RAW_SOURCE_VISIBLE,
                source_label=RAW_SOURCE_LABELS[RAW_SOURCE_VISIBLE],
                tracked_area_ids=tracked_area_ids,
                should_download=bool(target_timestamps[RAW_SOURCE_VISIBLE]),
                reason=(
                    "Ativo nos slots úteis de B2 da timeline atual"
                    if target_timestamps[RAW_SOURCE_VISIBLE]
                    else "Sem slots úteis de B2 na timeline atual"
                    if tracked_area_ids
                    else "Nenhum município acompanhado"
                ),
                target_timestamps=target_timestamps[RAW_SOURCE_VISIBLE],
            ),
            DownloadSourcePlan(
                source_key=RAW_SOURCE_INFRARED,
                source_label=RAW_SOURCE_LABELS[RAW_SOURCE_INFRARED],
                tracked_area_ids=tracked_area_ids,
                should_download=bool(target_timestamps[RAW_SOURCE_INFRARED]),
                reason=(
                    "Ativo nos slots úteis de B13 da timeline atual"
                    if target_timestamps[RAW_SOURCE_INFRARED]
                    else "Sem slots úteis de B13 na timeline atual"
                    if tracked_area_ids
                    else "Nenhum município acompanhado"
                ),
                target_timestamps=target_timestamps[RAW_SOURCE_INFRARED],
            ),
            DownloadSourcePlan(
                source_key=RAW_SOURCE_LIGHTNING,
                source_label=RAW_SOURCE_LABELS[RAW_SOURCE_LIGHTNING],
                tracked_area_ids=tracked_area_ids,
                should_download=bool(target_timestamps[RAW_SOURCE_LIGHTNING]),
                reason=(
                    "Ativo nos slots úteis de descargas da timeline atual"
                    if target_timestamps[RAW_SOURCE_LIGHTNING]
                    else "Sem slots úteis na timeline atual"
                    if tracked_area_ids
                    else "Nenhum município acompanhado"
                ),
                target_timestamps=target_timestamps[RAW_SOURCE_LIGHTNING],
            ),
        ]

    def _tracked_catalog_areas(self) -> list[AreaCatalogEntry]:
        return [
            area
            for tracked in self.state_store.list_tracked()
            if (area := self.catalog.get(tracked.area_id)) is not None
        ]

    def _timeline_reference_moment(self) -> datetime:
        return floor_to_slot(
            datetime.now(UTC) - timedelta(minutes=SLOT_MINUTES),
            slot_minutes=SLOT_MINUTES,
        )

    def _build_area_timeline_plan(
        self,
        area: AreaCatalogEntry,
        *,
        reference_moment: datetime | None = None,
    ) -> AreaTimelinePlan:
        reference_moment = reference_moment or self._timeline_reference_moment()
        end_slot = floor_to_slot(reference_moment, slot_minutes=SLOT_MINUTES)
        start_slot = end_slot - timedelta(
            minutes=SLOT_MINUTES * (self.settings.frame_count - 1)
        )
        frames: list[TimelineFrame] = []

        for index in range(self.settings.frame_count):
            slot_moment = start_slot + timedelta(minutes=SLOT_MINUTES * index)
            slot_timestamp = datetime_to_slot_timestamp(slot_moment)
            sunrise_alpha = self._sunrise_transition_alpha(area, slot_moment)
            sunset_alpha = self._sunset_transition_alpha(area, slot_moment)

            if sunrise_alpha is not None:
                frames.append(
                    TimelineFrame(
                        slot_timestamp=slot_timestamp,
                        phase=PHASE_SUNRISE_BLEND,
                        primary_source=SOURCE_INFRARED,
                        blend_source=SOURCE_VISIBLE,
                        blend_alpha=sunrise_alpha,
                        required_sources=(
                            SOURCE_INFRARED,
                            SOURCE_VISIBLE,
                            SOURCE_LIGHTNING,
                        ),
                    )
                )
                continue

            if sunset_alpha is not None:
                frames.append(
                    TimelineFrame(
                        slot_timestamp=slot_timestamp,
                        phase=PHASE_SUNSET_BLEND,
                        primary_source=SOURCE_VISIBLE,
                        blend_source=SOURCE_INFRARED,
                        blend_alpha=sunset_alpha,
                        required_sources=(
                            SOURCE_VISIBLE,
                            SOURCE_INFRARED,
                            SOURCE_LIGHTNING,
                        ),
                    )
                )
                continue

            if self._prefers_visible(area, slot_moment):
                frames.append(
                    TimelineFrame(
                        slot_timestamp=slot_timestamp,
                        phase=PHASE_VISIBLE,
                        primary_source=SOURCE_VISIBLE,
                        blend_source=None,
                        blend_alpha=None,
                        required_sources=(SOURCE_VISIBLE, SOURCE_LIGHTNING),
                    )
                )
                continue

            frames.append(
                TimelineFrame(
                    slot_timestamp=slot_timestamp,
                    phase=PHASE_INFRARED,
                    primary_source=SOURCE_INFRARED,
                    blend_source=None,
                    blend_alpha=None,
                    required_sources=(SOURCE_INFRARED, SOURCE_LIGHTNING),
                )
            )

        return AreaTimelinePlan(area_id=area.area_id, frames=tuple(frames))

    def _build_global_target_timestamps(
        self,
        tracked_areas: list[AreaCatalogEntry],
        *,
        reference_moment: datetime | None = None,
    ) -> dict[str, tuple[str, ...]]:
        targets: dict[str, set[str]] = {
            RAW_SOURCE_VISIBLE: set(),
            RAW_SOURCE_INFRARED: set(),
            RAW_SOURCE_LIGHTNING: set(),
        }
        reference_moment = reference_moment or self._timeline_reference_moment()

        for area in tracked_areas:
            plan = self._build_area_timeline_plan(area, reference_moment=reference_moment)
            for frame in plan.frames:
                for source_key in frame.required_sources:
                    targets[source_key].add(frame.slot_timestamp)

        return {
            source_key: tuple(sorted(values, reverse=True))
            for source_key, values in targets.items()
        }

    def _resolve_area_centroid(self, area: AreaCatalogEntry) -> tuple[float, float]:
        cached = self._centroid_cache.get(area.area_id)
        if cached is not None:
            return cached
        geometry = self.geometry_store.load_geometry(area)
        self._centroid_cache[area.area_id] = geometry.centroid
        return geometry.centroid

    def _build_frame_specs(self, area: AreaCatalogEntry) -> list[FrameSpec]:
        source_paths = self._available_raw_paths_by_source()
        lightning_points_by_timestamp = self._available_lightning_points_by_timestamp()
        timeline = self._build_area_timeline_plan(area)
        frame_specs: list[FrameSpec] = []

        for timeline_frame in timeline.frames:
            timestamp = timeline_frame.slot_timestamp
            visible_path = source_paths[RAW_SOURCE_VISIBLE].get(timestamp)
            infrared_path = source_paths[RAW_SOURCE_INFRARED].get(timestamp)
            lightning_points = lightning_points_by_timestamp.get(timestamp, ())

            if (
                timeline_frame.phase == PHASE_SUNRISE_BLEND
                and visible_path is not None
                and infrared_path is not None
            ):
                frame_specs.append(
                    FrameSpec(
                        timestamp=timestamp,
                        primary_path=infrared_path,
                        blend_path=visible_path,
                        blend_alpha=timeline_frame.blend_alpha or 0.0,
                        lightning_points=lightning_points,
                    )
                )
                continue

            if (
                timeline_frame.phase == PHASE_SUNSET_BLEND
                and visible_path is not None
                and infrared_path is not None
            ):
                frame_specs.append(
                    FrameSpec(
                        timestamp=timestamp,
                        primary_path=visible_path,
                        blend_path=infrared_path,
                        blend_alpha=timeline_frame.blend_alpha or 0.0,
                        lightning_points=lightning_points,
                    )
                )
                continue

            primary_path = self._resolve_primary_path_for_timeline_frame(
                timeline_frame,
                visible_path=visible_path,
                infrared_path=infrared_path,
            )
            if primary_path is None:
                continue

            frame_specs.append(
                FrameSpec(
                    timestamp=timestamp,
                    primary_path=primary_path,
                    lightning_points=lightning_points,
                )
            )

        return frame_specs

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

    @staticmethod
    def _latest_kept_timestamp(paths: list[Path]) -> str | None:
        timestamps = [parse_goes_timestamp(path.name) for path in paths]
        usable = [timestamp for timestamp in timestamps if _goes_timestamp_to_datetime(timestamp) is not None]
        if not usable:
            return None
        return max(usable)

    @staticmethod
    def _resolve_primary_path_for_timeline_frame(
        timeline_frame,
        *,
        visible_path: Path | None,
        infrared_path: Path | None,
    ) -> Path | None:
        if timeline_frame.primary_source == SOURCE_VISIBLE:
            return visible_path or infrared_path
        if timeline_frame.primary_source == SOURCE_INFRARED:
            return infrared_path or visible_path
        return visible_path or infrared_path

    def _prefers_visible(self, area: AreaCatalogEntry, moment_utc: datetime) -> bool:
        centroid = self._resolve_area_centroid(area)
        return is_within_visible_window(
            longitude=centroid[0],
            latitude=centroid[1],
            moment_utc=moment_utc,
            margin_hours=self.settings.solar_margin_hours,
        ).is_open

    def _sunset_transition_alpha(
        self,
        area: AreaCatalogEntry,
        moment_utc: datetime,
    ) -> float | None:
        centroid = self._resolve_area_centroid(area)
        return sunset_transition_alpha(
            longitude=centroid[0],
            latitude=centroid[1],
            moment_utc=moment_utc,
            blend_weights=TRANSITION_BLEND_WEIGHTS,
            slot_minutes=SLOT_MINUTES,
        )

    def _sunrise_transition_alpha(
        self,
        area: AreaCatalogEntry,
        moment_utc: datetime,
    ) -> float | None:
        centroid = self._resolve_area_centroid(area)
        return sunrise_transition_alpha(
            longitude=centroid[0],
            latitude=centroid[1],
            moment_utc=moment_utc,
            blend_weights=TRANSITION_BLEND_WEIGHTS,
            slot_minutes=SLOT_MINUTES,
        )

    def _source_download_summary(self, source_key: str, raw_frame_count: int) -> str:
        status = self._download_status[source_key]
        phase = str(status.get("phase") or "idle")
        attempted = int(status.get("attempted_count") or 0)
        completed = int(status.get("completed_count") or 0)
        failed = int(status.get("failed_count") or 0)
        active = int(status.get("active_count") or 0)
        current_file = status.get("current_file")
        reason = str(status.get("schedule_reason") or "")

        if phase == "disabled":
            return reason or "Fonte desativada"
        if phase == "paused":
            return reason or "Fonte pausada"
        if phase == "downloading":
            detail = f"{completed}/{attempted}" if attempted else "iniciando"
            return f"Baixando ({detail}, {active} ativos)"
        if phase == "processing":
            detail = f"{completed}/{attempted}" if attempted else "iniciando"
            return f"Convertendo ({detail}, {active} ativos)"
        if phase == "partial":
            if reason:
                return f"{raw_frame_count} arquivo(s) em disco; {reason.lower()}"
            return f"{raw_frame_count} arquivo(s) em disco; {failed} falha(s) no último ciclo"
        if phase == "ready":
            return f"{raw_frame_count} arquivo(s) brutos em disco"
        if phase == "error":
            return reason or f"Falha no download; {failed} erro(s)"
        if raw_frame_count:
            return f"{raw_frame_count} arquivo(s) brutos em disco"
        return reason or "Aguardando primeiro download"

    def _initial_download_status(self, source_key: str) -> dict[str, object]:
        return {
            "source_key": source_key,
            "phase": "disabled",
            "attempted_count": 0,
            "completed_count": 0,
            "failed_count": 0,
            "active_count": 0,
            "current_file": None,
            "last_downloaded": None,
            "latest_available": None,
            "active_downloads": [],
            "schedule_reason": "Aguardando municípios acompanhados",
            "is_relevant": False,
        }

    def _all_raw_files(self) -> list[dict[str, object]]:
        all_files: list[dict[str, object]] = []
        for source_key in self._downloaders:
            all_files.extend(self._raw_files_for_source(source_key))
        all_files.sort(
            key=lambda item: parse_goes_timestamp(str(item["filename"])),
            reverse=True,
        )
        return all_files

    def _raw_timestamp_count(self) -> int:
        return len(
            {
                parse_goes_timestamp(str(item["filename"]))
                for item in self._all_raw_files()
                if parse_goes_timestamp(str(item["filename"]))
            }
        )

    def _raw_disk_usage_bytes(self) -> int:
        total = 0
        for source_key in self._downloaders:
            for item in self._raw_files_for_source(source_key):
                total += int(item["size_bytes"])
        return total

    def _raw_files_for_source(self, source_key: str) -> list[dict[str, object]]:
        raw_dir = self._raw_dir_for_source(source_key)
        files = sorted(
            self._raw_files_in_dir(raw_dir, source_key),
            key=lambda path: path.name,
            reverse=True,
        )
        entries = []
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

    def _raw_dir_for_area(self, area: AreaCatalogEntry) -> Path:
        return self.settings.raw_dir

    def _raw_dir_for_source(self, source_key: str) -> Path:
        return self.settings.raw_dir / source_key

    def _available_raw_paths_by_source(self) -> dict[str, dict[str, Path]]:
        paths_by_source: dict[str, dict[str, Path]] = {}
        for source_key in (RAW_SOURCE_VISIBLE, RAW_SOURCE_INFRARED):
            entries: dict[str, Path] = {}
            for path in self._raw_dir_for_source(source_key).glob("*.tif"):
                if not self._is_valid_raw(path):
                    continue
                entries[parse_goes_timestamp(path.name)] = path
            paths_by_source[source_key] = entries
        return paths_by_source

    def _available_lightning_points_by_timestamp(
        self,
    ) -> dict[str, tuple[tuple[float, float], ...]]:
        points_by_timestamp: dict[str, tuple[tuple[float, float], ...]] = {}
        for path in self._raw_dir_for_source(RAW_SOURCE_LIGHTNING).glob("*.json"):
            timestamp = parse_goes_timestamp(path.name)
            if _goes_timestamp_to_datetime(timestamp) is None:
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
        return points_by_timestamp

    def _required_sources_for_timestamp(
        self,
        area_entries: list[AreaCatalogEntry],
        timestamp: str,
        source_paths: dict[str, dict[str, Path]],
    ) -> set[str]:
        moment_utc = _goes_timestamp_to_datetime(timestamp)
        if moment_utc is None:
            return set()

        visible_path = source_paths[RAW_SOURCE_VISIBLE].get(timestamp)
        infrared_path = source_paths[RAW_SOURCE_INFRARED].get(timestamp)
        lightning_path = source_paths.get(RAW_SOURCE_LIGHTNING, {}).get(timestamp)
        required_sources: set[str] = set()

        for area in area_entries:
            sunrise_alpha = self._sunrise_transition_alpha(area, moment_utc)
            sunset_alpha = self._sunset_transition_alpha(area, moment_utc)
            if sunrise_alpha is not None or sunset_alpha is not None:
                if visible_path is not None:
                    required_sources.add(RAW_SOURCE_VISIBLE)
                if infrared_path is not None:
                    required_sources.add(RAW_SOURCE_INFRARED)
                continue

            prefers_visible = self._prefers_visible(area, moment_utc)
            if prefers_visible:
                if visible_path is not None:
                    required_sources.add(RAW_SOURCE_VISIBLE)
                elif infrared_path is not None:
                    required_sources.add(RAW_SOURCE_INFRARED)
            else:
                if infrared_path is not None:
                    required_sources.add(RAW_SOURCE_INFRARED)
                elif visible_path is not None:
                    required_sources.add(RAW_SOURCE_VISIBLE)

        if lightning_path is not None and required_sources:
            required_sources.add(RAW_SOURCE_LIGHTNING)

        return required_sources

    def _build_global_raw_keep_set(
        self,
        *,
        reference_moment: datetime | None = None,
    ) -> dict[str, set[str]]:
        tracked_areas = self._tracked_catalog_areas()
        target_timestamps = self._build_global_target_timestamps(
            tracked_areas,
            reference_moment=reference_moment,
        )
        return {source_key: set(values) for source_key, values in target_timestamps.items()}

    def _prune_raw_cache(self, *, reference_moment: datetime | None = None) -> str | None:
        keep_by_source = self._build_global_raw_keep_set(reference_moment=reference_moment)
        for source_key in self._downloaders:
            raw_dir = self._raw_dir_for_source(source_key)
            keep_timestamps = keep_by_source[source_key]
            for file_path in self._raw_files_in_dir(raw_dir, source_key):
                if parse_goes_timestamp(file_path.name) not in keep_timestamps:
                    file_path.unlink(missing_ok=True)

            source_dir = self.settings.source_dir / source_key
            for file_path in source_dir.glob("*.nc"):
                output_name = self._downloaders[source_key].output_filename(file_path.name)
                if parse_goes_timestamp(output_name) not in keep_timestamps:
                    file_path.unlink(missing_ok=True)
            for file_path in source_dir.glob("*.nc.part"):
                source_name = file_path.name.removesuffix(".part")
                output_name = self._downloaders[source_key].output_filename(source_name)
                if parse_goes_timestamp(output_name) not in keep_timestamps:
                    file_path.unlink(missing_ok=True)

        retained_files = [
            path
            for source_key in self._downloaders
            for path in self._raw_dir_for_source(source_key).glob("*.tif")
        ]
        return self._latest_kept_timestamp(retained_files)

    @staticmethod
    def _raw_files_in_dir(raw_dir: Path, source_key: str) -> list[Path]:
        if source_key == RAW_SOURCE_LIGHTNING:
            files = list(raw_dir.glob("*.json"))
        else:
            files = list(raw_dir.glob("*.tif"))
        if files:
            return files
        return list(raw_dir.glob("*.nc"))

    @staticmethod
    def _disk_warning(free_bytes: int) -> str | None:
        if free_bytes < 10 * 1024 * 1024 * 1024:
            return "Espaço livre muito baixo em disco"
        if free_bytes < 20 * 1024 * 1024 * 1024:
            return "Espaço livre em disco abaixo do ideal"
        return None


def _download_phase_label(phase: str) -> str:
    mapping = {
        "disabled": "desativado",
        "paused": "pausado",
        "idle": "ocioso",
        "downloading": "baixando",
        "processing": "convertendo",
        "partial": "parcial",
        "ready": "pronto",
        "error": "erro",
    }
    return mapping.get(phase, phase)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _goes_timestamp_to_datetime(timestamp: str) -> datetime | None:
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


def _output_name_for_source_file(source_filename: str) -> str:
    return f"{Path(source_filename).stem}.tif"


def _point_within_polygon(
    point: tuple[float, float],
    polygon: tuple[tuple[float, float], ...],
) -> bool:
    if len(polygon) < 3:
        return False

    vertices = list(polygon)
    if vertices[0] != vertices[-1]:
        vertices.append(vertices[0])

    for start, end in zip(vertices, vertices[1:]):
        if _point_on_segment(point, start, end):
            return True

    point_lon, point_lat = point
    inside = False
    for (lon_a, lat_a), (lon_b, lat_b) in zip(vertices, vertices[1:]):
        intersects = ((lat_a > point_lat) != (lat_b > point_lat)) and (
            point_lon < (lon_b - lon_a) * (point_lat - lat_a) / (lat_b - lat_a) + lon_a
        )
        if intersects:
            inside = not inside
    return inside


def _point_on_segment(
    point: tuple[float, float],
    segment_start: tuple[float, float],
    segment_end: tuple[float, float],
    *,
    tolerance: float = 1e-9,
) -> bool:
    point_lon, point_lat = point
    start_lon, start_lat = segment_start
    end_lon, end_lat = segment_end

    cross_product = (
        (point_lat - start_lat) * (end_lon - start_lon)
        - (point_lon - start_lon) * (end_lat - start_lat)
    )
    if abs(cross_product) > tolerance:
        return False

    min_lon = min(start_lon, end_lon) - tolerance
    max_lon = max(start_lon, end_lon) + tolerance
    min_lat = min(start_lat, end_lat) - tolerance
    max_lat = max(start_lat, end_lat) + tolerance
    return min_lon <= point_lon <= max_lon and min_lat <= point_lat <= max_lat
