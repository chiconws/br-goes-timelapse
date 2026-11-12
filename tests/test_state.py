"""Tests for the SQLite state store."""

from __future__ import annotations

import sqlite3

from goes_timelapse.core.state import StateStore
from goes_timelapse.data.catalog import AreaCatalog


def test_state_store_migrates_legacy_tracked_municipalities_table(settings):
    """Test state store migrates legacy tracked municipalities table."""
    connection = sqlite3.connect(settings.db_path)
    connection.execute(
        """
        CREATE TABLE tracked_municipalities (
            ibge_code TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            state TEXT NOT NULL,
            status TEXT NOT NULL,
            tracked_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_error TEXT,
            latest_source_timestamp TEXT,
            gif_path TEXT,
            snippet_path TEXT
        )
        """
    )
    connection.execute(
        """
        INSERT INTO tracked_municipalities (
            ibge_code, name, state, status, tracked_at, updated_at,
            last_error, latest_source_timestamp, gif_path, snippet_path
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "3205309",
            "Vitoria",
            "ES",
            "ready",
            "2026-03-11T00:00:00+00:00",
            "2026-03-11T00:00:00+00:00",
            None,
            None,
            "/media/goes_timelapse/municipio-3205309.webp",
            "/config/goes_timelapse/lovelace/municipio-3205309.yaml",
        ),
    )
    connection.commit()
    connection.close()

    store = StateStore(settings.db_path)
    tracked = store.list_tracked()
    store.close()

    assert len(tracked) == 1
    assert tracked[0].area_id == "municipio-3205309"
    assert tracked[0].area_type == "municipio"


def test_state_store_upserts_and_reads_tracked_area(settings):
    """Test state store upserts and reads tracked area."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    area = catalog.get("municipio-3205309")
    assert area is not None

    store = StateStore(settings.db_path)
    store.upsert_tracked(area, status="queued")
    tracked = store.get_tracked(area.area_id)
    store.close()

    assert tracked is not None
    assert tracked.area_id == area.area_id
    assert tracked.status == "queued"


def test_state_store_migrates_marker_columns_for_existing_tracked_areas_table(settings):
    """Test state store migrates marker columns for existing tracked areas table."""
    connection = sqlite3.connect(settings.db_path)
    connection.execute(
        """
        CREATE TABLE tracked_areas (
            area_id TEXT PRIMARY KEY,
            area_type TEXT NOT NULL,
            area_code TEXT NOT NULL,
            name TEXT NOT NULL,
            state_code TEXT,
            state_name TEXT,
            parent_name TEXT,
            status TEXT NOT NULL,
            tracked_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_error TEXT,
            latest_source_timestamp TEXT,
            media_path TEXT,
            snippet_path TEXT
        )
        """
    )
    connection.commit()
    connection.close()

    store = StateStore(settings.db_path)
    columns = {
        row[1]
        for row in store._connection.execute(
            "PRAGMA table_info(tracked_areas)"
        ).fetchall()
    }
    store.close()

    assert "marker_lat" in columns
    assert "marker_lon" in columns


def test_state_store_persists_marker_coordinates(settings):
    """Test state store persists marker coordinates."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    area = catalog.get("municipio-3205309")
    assert area is not None

    store = StateStore(settings.db_path)
    store.upsert_tracked(area, status="queued")
    store.set_marker(area.area_id, marker_lat=-20.321, marker_lon=-40.337)
    tracked = store.get_tracked(area.area_id)
    store.close()

    assert tracked is not None
    assert tracked.marker_lat == -20.321
    assert tracked.marker_lon == -40.337
