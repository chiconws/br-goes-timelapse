"""Tests for the catalog build script."""

from __future__ import annotations

from goes_timelapse.data.ibge import _geometry_from_geojson
from scripts.build_area_catalog import _build_area_rows


def test_geometry_loader_uses_largest_polygon_from_multipolygon():
    """Test geometry loader uses largest polygon from multipolygon."""
    payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"codarea": "3205309"},
                "geometry": {
                    "type": "MultiPolygon",
                    "coordinates": [
                        [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]],
                        [[[0.0, 0.0], [4.0, 0.0], [4.0, 4.0], [0.0, 4.0], [0.0, 0.0]]],
                    ],
                },
            }
        ],
    }

    geometry = _geometry_from_geojson("municipio-3205309", payload)

    assert geometry.bounds == (0.0, 0.0, 4.0, 4.0)
    assert geometry.centroid == (2.0, 2.0)


def test_build_area_rows_includes_only_supported_area_types():
    """Test build area rows includes only supported area types."""
    payloads = {
        "estados": [
            {
                "id": 32,
                "sigla": "ES",
                "nome": "Espirito Santo",
                "regiao": {"id": 3, "sigla": "SE", "nome": "Sudeste"},
            }
        ],
        "municipios": [
            {
                "id": 3205309,
                "nome": "Vitoria",
                "regiao-imediata": {
                    "id": 320001,
                    "nome": "Vitoria",
                    "regiao-intermediaria": {
                        "id": 3201,
                        "nome": "Vitoria",
                        "UF": {"id": 32, "sigla": "ES", "nome": "Espirito Santo"},
                    },
                },
            }
        ],
    }

    rows = _build_area_rows(payloads, population_by_ibge={"3205309": 322869})
    area_ids = {row["area_id"] for row in rows}

    assert area_ids == {"municipio-3205309"}
