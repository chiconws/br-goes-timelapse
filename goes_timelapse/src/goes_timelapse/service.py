from __future__ import annotations

import asyncio
import logging
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from rasterio.windows import Window

from goes_timelapse.catalog import AreaCatalog
from goes_timelapse.config import Settings
from goes_timelapse.downloader import DownloadReport, GoesDownloader
from goes_timelapse.ibge import IbgeGeometryStore
from goes_timelapse.models import AreaCatalogEntry, RenderedArea, TrackedArea
from goes_timelapse.raster_sources import open_raster_source
from goes_timelapse.rendering import (
    AreaRenderer,
    WebpBuilder,
    parse_goes_timestamp,
    write_lovelace_snippet,
)
from goes_timelapse.solar import is_within_visible_window
from goes_timelapse.state import StateStore


LOGGER = logging.getLogger(__name__)

RAW_SOURCE_VISIBLE = "visible"
RAW_SOURCE_LABELS = {
    RAW_SOURCE_VISIBLE: "Visível B2",
}


@dataclass(slots=True, frozen=True)
class DownloadSourcePlan:
    source_key: str
    source_label: str
    tracked_area_ids: tuple[str, ...]
    should_download: bool
    reason: str


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
                source_dir=settings.source_dir,
                raw_dir=settings.raw_dir,
                raw_history=settings.raw_history,
                progress_callback=lambda payload: self._update_raw_download_status(
                    RAW_SOURCE_VISIBLE, payload
                ),
            ),
        }
        self.renderer = AreaRenderer(settings)
        self.webp_builder = WebpBuilder(settings)
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._queued_ids: set[str] = set()
        self._tasks: list[asyncio.Task[None]] = []
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
        for area_id in self.state_store.tracked_ids():
            if self.catalog.get(area_id) is None:
                LOGGER.info("Removing unsupported tracked area %s", area_id)
                self.state_store.remove_tracked(area_id)
                self._cleanup_area_files(area_id)
                continue
            self.state_store.set_status(area_id, "queued", last_error=None)
            await self.enqueue(area_id)
        if self._start_background_tasks:
            self._tasks.append(asyncio.create_task(self._poll_loop(), name="goes-poller"))
            self._tasks.append(asyncio.create_task(self._worker_loop(), name="goes-worker"))

    async def stop(self) -> None:
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
        if (
            not self.state_store.is_tracked(area_id)
            and self.state_store.count_tracked() >= self.settings.max_tracked
        ):
            raise ValueError(
                f"Limite máximo de municípios acompanhados atingido ({self.settings.max_tracked})"
            )
        self.state_store.upsert_tracked(area, status="queued")
        await self.enqueue(area_id)
        tracked = self.state_store.get_tracked(area_id)
        assert tracked is not None
        return tracked

    async def remove_tracked(self, area_id: str) -> None:
        self.state_store.remove_tracked(area_id)
        self._queued_ids.discard(area_id)
        self._cleanup_area_files(area_id)

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
            if source["is_relevant"] or source["phase"] == "downloading"
        ]
        return {
            **self._status,
            "tracked_count": self.state_store.count_tracked(),
            "queue_length": self._queue.qsize(),
            "raw_frame_count": len(files),
            "raw_frame_latest": files[0]["label"] if files else None,
            "raw_download_summary": " | ".join(summaries) if summaries else "Nenhuma fonte ativa",
            "raw_disk_usage_bytes": raw_disk_bytes,
            "disk_free_bytes": data_usage.free,
            "disk_total_bytes": data_usage.total,
            "disk_warning": self._disk_warning(data_usage.free),
        }

    def downloads_snapshot(self) -> dict[str, object]:
        sources = []
        for source_key in (RAW_SOURCE_VISIBLE,):
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
        self._status["last_poll_started_at"] = _utc_now()
        self._status["last_poll_error"] = None
        self._status["last_poll_new_downloads"] = 0
        plans = await self._build_download_plans()
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
                    report = await self._downloaders[plan.source_key].refresh_latest()
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

                if report.kept_files and report.downloaded_count > 0:
                    for area_id in plan.tracked_area_ids:
                        self.state_store.set_status(area_id, "queued", last_error=None)
                        await self.enqueue(area_id)
        finally:
            self._status["last_poll_finished_at"] = _utc_now()

        self._status["last_poll_new_downloads"] = total_downloads
        if error_messages:
            self._status["last_poll_error"] = " | ".join(error_messages)

    async def _worker_loop(self) -> None:
        while True:
            area_id = await self._queue.get()
            self._queued_ids.discard(area_id)
            try:
                if self.state_store.is_tracked(area_id):
                    await asyncio.to_thread(self._process_area, area_id)
            except Exception:  # pragma: no cover
                LOGGER.exception("Area processing crashed for %s", area_id)
                self.state_store.set_status(
                    area_id,
                    "error",
                    last_error="Falha inesperada no processamento",
                )
            finally:
                self._queue.task_done()

    def _process_area(self, area_id: str) -> RenderedArea | None:
        area = self.catalog.get(area_id)
        if area is None:
            self.state_store.set_status(area_id, "error", last_error="Área não encontrada")
            return None

        raw_paths = sorted(
            self._raw_files_in_dir(self._raw_dir_for_area(area)),
            key=lambda path: parse_goes_timestamp(path.name),
        )[-self.settings.frame_count :]
        raw_paths = [path for path in raw_paths if self._is_valid_raw(path)]
        if not raw_paths:
            self.state_store.set_status(area_id, "queued", last_error=None)
            return None

        self.state_store.set_status(area_id, "processing", last_error=None)
        try:
            geometry = self.geometry_store.load_geometry(area)
            png_paths = self.renderer.process_frames(area, geometry, raw_paths)
            if not png_paths:
                self.state_store.set_status(
                    area_id, "error", last_error="Nenhum quadro foi renderizado"
                )
                return None

            media_path = self.webp_builder.build(area_id, png_paths)
            snippet_path = write_lovelace_snippet(
                self.settings.snippets_dir,
                area,
                area_id,
            )
            latest_source_timestamp = parse_goes_timestamp(raw_paths[-1].name)

            if not self.state_store.is_tracked(area_id):
                self._cleanup_area_files(area_id)
                return None

            self.state_store.set_status(
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
            self.state_store.set_status(area_id, "error", last_error=str(err))
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
        status["latest_available"] = None

    async def _build_download_plans(self) -> list[DownloadSourcePlan]:
        tracked = self.state_store.list_tracked()
        visible_ids = tuple(area.area_id for area in tracked if area.area_type == "municipio")

        plans = [
            DownloadSourcePlan(
                source_key=RAW_SOURCE_VISIBLE,
                source_label=RAW_SOURCE_LABELS[RAW_SOURCE_VISIBLE],
                tracked_area_ids=visible_ids,
                should_download=False,
                reason="Nenhum município acompanhado",
            ),
        ]

        if not visible_ids:
            return plans

        now_utc = datetime.now(UTC)
        visible_open = False
        for area_id in visible_ids:
            area = self.catalog.get(area_id)
            if area is None:
                continue
            centroid = await asyncio.to_thread(self._resolve_area_centroid, area)
            solar_window = is_within_visible_window(
                longitude=centroid[0],
                latitude=centroid[1],
                moment_utc=now_utc,
                margin_hours=self.settings.solar_margin_hours,
            )
            if solar_window.is_open:
                visible_open = True
                break

        plans[0] = DownloadSourcePlan(
            source_key=RAW_SOURCE_VISIBLE,
            source_label=RAW_SOURCE_LABELS[RAW_SOURCE_VISIBLE],
            tracked_area_ids=visible_ids,
            should_download=visible_open,
            reason=(
                "Ativo na janela solar dos municípios"
                if visible_open
                else (
                    "Pausado fora da janela solar dos municípios "
                    f"(margem de {self.settings.solar_margin_hours}h)"
                )
            ),
        )
        return plans

    def _resolve_area_centroid(self, area: AreaCatalogEntry) -> tuple[float, float]:
        cached = self._centroid_cache.get(area.area_id)
        if cached is not None:
            return cached
        geometry = self.geometry_store.load_geometry(area)
        self._centroid_cache[area.area_id] = geometry.centroid
        return geometry.centroid

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
            if current_file:
                return f"Baixando ({detail}, {active} ativos): {current_file}"
            return f"Baixando ({detail}, {active} ativos)"
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
        all_files = self._raw_files_for_source(RAW_SOURCE_VISIBLE)
        all_files.sort(key=lambda item: str(item["filename"]), reverse=True)
        return all_files

    def _raw_disk_usage_bytes(self) -> int:
        total = 0
        for source_key in self._downloaders:
            for item in self._raw_files_for_source(source_key):
                total += int(item["size_bytes"])
        return total

    def _raw_files_for_source(self, source_key: str) -> list[dict[str, object]]:
        raw_dir = self._raw_dir_for_source(source_key)
        files = sorted(self._raw_files_in_dir(raw_dir), key=lambda path: path.name, reverse=True)
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
        return self.settings.raw_dir

    @staticmethod
    def _raw_files_in_dir(raw_dir: Path) -> list[Path]:
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
        "partial": "parcial",
        "ready": "pronto",
        "error": "erro",
    }
    return mapping.get(phase, phase)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()
