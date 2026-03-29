from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from goes_timelapse.catalog import AreaCatalog
from goes_timelapse.config import Settings
from goes_timelapse.service import GoesTimelapseService
from goes_timelapse.state import StateStore

STATIC_ASSET_VERSION = "20260328-1"
STATIC_APP_JS_ROUTE = f"/static/app-{STATIC_ASSET_VERSION}.js"
STATIC_STYLES_ROUTE = f"/static/styles-{STATIC_ASSET_VERSION}.css"


class MarkerPayload(BaseModel):
    lat: float
    lon: float


def create_app(
    settings: Settings | None = None,
    *,
    start_background_tasks: bool = True,
) -> FastAPI:
    app_settings = settings or Settings.from_env()
    _configure_logging(app_settings.log_level)
    static_dir = Path(__file__).resolve().parent / "static"
    index_path = static_dir / "index.html"
    app_js_path = static_dir / "app.js"
    styles_path = static_dir / "styles.css"
    index_template = index_path.read_text(encoding="utf-8")
    no_store_headers = {"Cache-Control": "no-store, no-cache, must-revalidate"}

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        service = GoesTimelapseService(
            settings=app_settings,
            catalog=AreaCatalog.from_path(app_settings.catalog_path),
            state_store=StateStore(app_settings.db_path),
            start_background_tasks=start_background_tasks,
        )
        app.state.service = service
        await service.start()
        try:
            yield
        finally:
            await service.stop()

    app = FastAPI(title="Timelapses GOES de Municípios", lifespan=lifespan)

    @app.middleware("http")
    async def ingress_only(request: Request, call_next):
        client_host = request.client.host if request.client else ""
        if client_host not in app_settings.allowed_client_hosts:
            return JSONResponse(status_code=403, content={"detail": "Acesso permitido apenas via Ingress"})
        return await call_next(request)

    @app.get("/")
    async def index(request: Request) -> HTMLResponse:
        base_href = _base_href_for_request(request)
        rendered_index = (
            index_template.replace("__BASE_PATH__", base_href)
            .replace("__STYLE_HREF__", f"{base_href}static/styles-{STATIC_ASSET_VERSION}.css")
            .replace("__SCRIPT_SRC__", f"{base_href}static/app-{STATIC_ASSET_VERSION}.js")
        )
        return HTMLResponse(
            rendered_index,
            headers=no_store_headers,
        )

    @app.get(STATIC_APP_JS_ROUTE)
    @app.get("/static/app.js")
    async def static_app_js() -> FileResponse:
        return FileResponse(
            app_js_path,
            media_type="application/javascript; charset=utf-8",
            headers=no_store_headers,
        )

    @app.get(STATIC_STYLES_ROUTE)
    @app.get("/static/styles.css")
    async def static_styles() -> FileResponse:
        return FileResponse(
            styles_path,
            media_type="text/css; charset=utf-8",
            headers=no_store_headers,
        )

    @app.get("/api/status")
    async def status(request: Request) -> dict[str, object]:
        service: GoesTimelapseService = request.app.state.service
        return service.status_snapshot()

    @app.get("/api/downloads")
    async def downloads(request: Request) -> dict[str, object]:
        service: GoesTimelapseService = request.app.state.service
        return service.downloads_snapshot()

    @app.get("/api/areas")
    async def areas(request: Request, q: str = "") -> list[dict[str, object]]:
        service: GoesTimelapseService = request.app.state.service
        return [_area_payload(area) for area in service.search(q)]

    @app.get("/api/municipalities")
    async def municipalities_alias(request: Request, q: str = "") -> list[dict[str, object]]:
        service: GoesTimelapseService = request.app.state.service
        return [_area_payload(area) for area in service.search(q)]

    @app.get("/api/tracked")
    async def tracked(request: Request) -> list[dict[str, object]]:
        service: GoesTimelapseService = request.app.state.service
        items = []
        for area in service.tracked():
            media_exists = bool(area.media_path) and service.media_path(area.area_id).exists()
            media_path = service.media_path(area.area_id)
            media_version = media_path.stat().st_mtime_ns if media_exists else None
            items.append(
                {
                    "area_id": area.area_id,
                    "area_type": area.area_type,
                    "area_code": area.area_code,
                    "name": area.name,
                    "display_name": area.display_name,
                    "type_label": area.type_label,
                    "code_label": area.code_label,
                    "status": area.status,
                    "last_error": area.last_error,
                    "latest_source_timestamp": area.latest_source_timestamp,
                    "media_exists": media_exists,
                    "marker_lat": area.marker_lat,
                    "marker_lon": area.marker_lon,
                    "media_source": (
                        f"media-source://media_source/local/goes_timelapse/{area.area_id}.webp"
                        if media_exists
                        else None
                    ),
                    "preview_url": (
                        f"api/media/{area.area_id}?m={media_version}" if media_exists else None
                    ),
                    "media_version": media_version,
                    "snippet_url": f"api/snippets/{area.area_id}",
                }
            )
        return items

    @app.put("/api/tracked/{area_id}")
    async def add_tracked(request: Request, area_id: str) -> dict[str, object]:
        service: GoesTimelapseService = request.app.state.service
        try:
            tracked_item = await service.add_tracked(area_id)
        except KeyError as err:
            raise HTTPException(status_code=404, detail="Município não encontrado") from err
        except ValueError as err:
            raise HTTPException(status_code=409, detail=str(err)) from err
        return {
            "area_id": tracked_item.area_id,
            "status": tracked_item.status,
        }

    @app.put("/api/tracked/{area_id}/marker")
    async def upsert_marker(
        request: Request,
        area_id: str,
        payload: MarkerPayload,
    ) -> dict[str, object]:
        service: GoesTimelapseService = request.app.state.service
        try:
            tracked_item = await service.set_marker(
                area_id,
                marker_lat=payload.lat,
                marker_lon=payload.lon,
            )
        except KeyError as err:
            raise HTTPException(status_code=404, detail="Município acompanhado não encontrado") from err
        except ValueError as err:
            raise HTTPException(status_code=422, detail=str(err)) from err

        return {
            "area_id": tracked_item.area_id,
            "status": tracked_item.status,
            "marker_lat": tracked_item.marker_lat,
            "marker_lon": tracked_item.marker_lon,
        }

    @app.delete("/api/tracked/{area_id}/marker")
    async def delete_marker(request: Request, area_id: str) -> dict[str, object]:
        service: GoesTimelapseService = request.app.state.service
        try:
            tracked_item = await service.clear_marker(area_id)
        except KeyError as err:
            raise HTTPException(status_code=404, detail="Município acompanhado não encontrado") from err

        return {
            "area_id": tracked_item.area_id,
            "status": tracked_item.status,
            "marker_lat": tracked_item.marker_lat,
            "marker_lon": tracked_item.marker_lon,
        }

    @app.delete("/api/tracked/{area_id}")
    async def delete_tracked(request: Request, area_id: str) -> dict[str, object]:
        service: GoesTimelapseService = request.app.state.service
        await service.remove_tracked(area_id)
        return {"removed": area_id}

    @app.get("/api/snippets/{area_id}")
    async def snippet(request: Request, area_id: str) -> dict[str, object]:
        service: GoesTimelapseService = request.app.state.service
        try:
            text = service.snippet_text(area_id)
        except FileNotFoundError as err:
            raise HTTPException(status_code=404, detail="Snippet não encontrado") from err
        return {"area_id": area_id, "snippet": text}

    @app.get("/api/media/{area_id}")
    async def media(request: Request, area_id: str) -> FileResponse:
        service: GoesTimelapseService = request.app.state.service
        media_path = service.media_path(area_id)
        if not media_path.exists():
            raise HTTPException(status_code=404, detail="Animação não encontrada")
        return FileResponse(
            media_path,
            media_type="image/webp",
            filename=f"{area_id}.webp",
            headers=no_store_headers,
        )

    return app


def main() -> None:
    settings = Settings.from_env()
    _configure_logging(settings.log_level)
    uvicorn.run(create_app(settings=settings), host=settings.host, port=settings.port)


def _area_payload(area) -> dict[str, object]:
    return {
        "area_id": area.area_id,
        "area_type": area.area_type,
        "area_code": area.area_code,
        "name": area.name,
        "display_name": area.display_name,
        "type_label": area.type_label,
        "code_label": area.code_label,
        "population": area.population,
        "state_code": area.state_code,
        "state_name": area.state_name,
        "parent_name": area.parent_name,
    }


def _configure_logging(level: int) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def _base_href_for_request(request: Request) -> str:
    ingress_path = request.headers.get("x-ingress-path", "").strip()
    if ingress_path:
        return ingress_path if ingress_path.endswith("/") else ingress_path + "/"

    root_path = str(request.scope.get("root_path") or "").strip()
    if root_path:
        return root_path if root_path.endswith("/") else root_path + "/"

    path = request.url.path.rstrip("/")
    if not path:
        return "/"
    return path + "/"
