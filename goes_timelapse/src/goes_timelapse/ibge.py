from __future__ import annotations

import gzip
import json
import math
import urllib.request
from pathlib import Path
from urllib.parse import quote

from goes_timelapse.models import AreaCatalogEntry, AreaGeometry


MALHAS_SEGMENTS = {
    "municipio": "municipios",
}


class IbgeGeometryStore:
    def __init__(self, cache_dir: Path, *, base_url: str, timeout_seconds: int = 30):
        self._cache_dir = cache_dir
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds

    def load_geometry(self, area: AreaCatalogEntry) -> AreaGeometry:
        cache_path = self.cache_path(area.area_id)
        if cache_path.exists():
            return self._read_geometry(cache_path)

        geometry = self.fetch_geometry(area)
        self._write_geometry(cache_path, geometry)
        return geometry

    def fetch_geometry(self, area: AreaCatalogEntry) -> AreaGeometry:
        segment = MALHAS_SEGMENTS[area.area_type]
        encoded_code = quote(area.area_code, safe="")
        url = (
            f"{self._base_url}/{segment}/{encoded_code}"
            "?formato=application%2Fvnd.geo%2Bjson"
        )
        payload = _load_json_url(url, timeout_seconds=self._timeout_seconds)
        return _geometry_from_geojson(area.area_id, payload)

    def cache_path(self, area_id: str) -> Path:
        return self._cache_dir / f"{area_id}.json.gz"

    @staticmethod
    def _read_geometry(path: Path) -> AreaGeometry:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
        return AreaGeometry(
            area_id=str(payload["area_id"]),
            centroid=(float(payload["centroid"][0]), float(payload["centroid"][1])),
            bounds=tuple(float(value) for value in payload["bounds"]),
            polygon=tuple((float(lon), float(lat)) for lon, lat in payload["polygon"]),
        )

    @staticmethod
    def _write_geometry(path: Path, geometry: AreaGeometry) -> None:
        with gzip.open(path, "wt", encoding="utf-8") as handle:
            json.dump(
                {
                    "area_id": geometry.area_id,
                    "centroid": [round(geometry.centroid[0], 6), round(geometry.centroid[1], 6)],
                    "bounds": [round(value, 6) for value in geometry.bounds],
                    "polygon": [
                        [round(lon, 6), round(lat, 6)] for lon, lat in geometry.polygon
                    ],
                },
                handle,
                ensure_ascii=False,
                separators=(",", ":"),
            )


def _load_json_url(url: str, *, timeout_seconds: int) -> object:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json, application/vnd.geo+json",
            "Accept-Encoding": "gzip",
            "User-Agent": "goes-timelapse-addon/0.1",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        raw = response.read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw.decode("utf-8"))


def _geometry_from_geojson(area_id: str, payload: object) -> AreaGeometry:
    if not isinstance(payload, dict):
        raise ValueError("Unexpected GeoJSON response shape")
    features = payload.get("features")
    if not isinstance(features, list) or not features:
        raise ValueError("No GeoJSON features returned by IBGE")
    geometry = features[0].get("geometry")
    if not isinstance(geometry, dict):
        raise ValueError("Malformed GeoJSON geometry")

    rings = _extract_exterior_rings(geometry)
    if not rings:
        raise ValueError("No polygon rings returned by IBGE")

    polygon = max(rings, key=lambda ring: abs(_polygon_area(ring)))
    bounds = _ring_bounds(polygon)
    centroid = _polygon_centroid(polygon)
    return AreaGeometry(
        area_id=area_id,
        centroid=(round(float(centroid[0]), 6), round(float(centroid[1]), 6)),
        bounds=tuple(round(float(value), 6) for value in bounds),
        polygon=tuple((round(float(lon), 6), round(float(lat), 6)) for lon, lat in polygon),
    )


def _extract_exterior_rings(geometry: dict[str, object]) -> list[list[tuple[float, float]]]:
    geometry_type = geometry.get("type")
    coordinates = geometry.get("coordinates")
    if geometry_type == "Polygon":
        if not isinstance(coordinates, list):
            return []
        return [_normalize_ring(coordinates[0])] if coordinates else []
    if geometry_type == "MultiPolygon":
        if not isinstance(coordinates, list):
            return []
        return [
            _normalize_ring(polygon[0])
            for polygon in coordinates
            if isinstance(polygon, list) and polygon
        ]
    raise ValueError(f"Unsupported GeoJSON geometry type: {geometry_type}")


def _normalize_ring(raw_ring: object) -> list[tuple[float, float]]:
    if not isinstance(raw_ring, list):
        return []
    ring = [(float(point[0]), float(point[1])) for point in raw_ring if len(point) >= 2]
    if not ring:
        return []
    if ring[0] != ring[-1]:
        ring.append(ring[0])
    return ring


def _ring_bounds(ring: list[tuple[float, float]]) -> tuple[float, float, float, float]:
    longitudes = [point[0] for point in ring]
    latitudes = [point[1] for point in ring]
    return (min(longitudes), min(latitudes), max(longitudes), max(latitudes))


def _polygon_area(ring: list[tuple[float, float]]) -> float:
    area = 0.0
    for index in range(len(ring) - 1):
        lon_a, lat_a = ring[index]
        lon_b, lat_b = ring[index + 1]
        area += lon_a * lat_b - lon_b * lat_a
    return area / 2.0


def _polygon_centroid(ring: list[tuple[float, float]]) -> tuple[float, float]:
    area = _polygon_area(ring)
    if math.isclose(area, 0.0, abs_tol=1e-12):
        bounds = _ring_bounds(ring)
        return ((bounds[0] + bounds[2]) / 2.0, (bounds[1] + bounds[3]) / 2.0)

    factor = 0.0
    centroid_lon = 0.0
    centroid_lat = 0.0
    for index in range(len(ring) - 1):
        lon_a, lat_a = ring[index]
        lon_b, lat_b = ring[index + 1]
        cross = lon_a * lat_b - lon_b * lat_a
        factor += cross
        centroid_lon += (lon_a + lon_b) * cross
        centroid_lat += (lat_a + lat_b) * cross
    if math.isclose(factor, 0.0, abs_tol=1e-12):
        bounds = _ring_bounds(ring)
        return ((bounds[0] + bounds[2]) / 2.0, (bounds[1] + bounds[3]) / 2.0)
    divisor = 3.0 * factor
    return (centroid_lon / divisor, centroid_lat / divisor)
