from __future__ import annotations

import sqlite3
import threading
from datetime import UTC, datetime
from pathlib import Path

from goes_timelapse.models import AreaCatalogEntry, TrackedArea


class StateStore:
    def __init__(self, db_path: Path):
        self._db_path = db_path
        self._lock = threading.RLock()
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(db_path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._initialize()

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def _initialize(self) -> None:
        with self._lock:
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS tracked_areas (
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
            self._maybe_migrate_municipality_table()
            self._connection.commit()

    def _maybe_migrate_municipality_table(self) -> None:
        cursor = self._connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'tracked_municipalities'"
        )
        if cursor.fetchone() is None:
            return

        tracked_areas_count = int(
            self._connection.execute("SELECT COUNT(*) FROM tracked_areas").fetchone()[0]
        )
        if tracked_areas_count > 0:
            return

        self._connection.execute(
            """
            INSERT INTO tracked_areas (
                area_id, area_type, area_code, name, state_code, state_name,
                parent_name, status, tracked_at, updated_at, last_error,
                latest_source_timestamp, media_path, snippet_path
            )
            SELECT
                'municipio-' || ibge_code,
                'municipio',
                ibge_code,
                name,
                state,
                NULL,
                NULL,
                status,
                tracked_at,
                updated_at,
                last_error,
                latest_source_timestamp,
                gif_path,
                snippet_path
            FROM tracked_municipalities
            """
        )

    def count_tracked(self) -> int:
        with self._lock:
            cursor = self._connection.execute("SELECT COUNT(*) FROM tracked_areas")
            return int(cursor.fetchone()[0])

    def list_tracked(self) -> list[TrackedArea]:
        with self._lock:
            cursor = self._connection.execute(
                """
                SELECT area_id, area_type, area_code, name, state_code, state_name, parent_name,
                       status, tracked_at, updated_at, last_error, latest_source_timestamp,
                       media_path, snippet_path
                FROM tracked_areas
                ORDER BY tracked_at ASC
                """
            )
            return [self._row_to_tracked(row) for row in cursor.fetchall()]

    def get_tracked(self, area_id: str) -> TrackedArea | None:
        with self._lock:
            cursor = self._connection.execute(
                """
                SELECT area_id, area_type, area_code, name, state_code, state_name, parent_name,
                       status, tracked_at, updated_at, last_error, latest_source_timestamp,
                       media_path, snippet_path
                FROM tracked_areas
                WHERE area_id = ?
                """,
                (area_id,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return self._row_to_tracked(row)

    def tracked_ids(self) -> list[str]:
        with self._lock:
            cursor = self._connection.execute(
                "SELECT area_id FROM tracked_areas ORDER BY tracked_at ASC"
            )
            return [str(row[0]) for row in cursor.fetchall()]

    def is_tracked(self, area_id: str) -> bool:
        return self.get_tracked(area_id) is not None

    def upsert_tracked(self, area: AreaCatalogEntry, status: str = "queued") -> None:
        now = self._now()
        with self._lock:
            existing = self.get_tracked(area.area_id)
            tracked_at = existing.tracked_at if existing is not None else now
            self._connection.execute(
                """
                INSERT INTO tracked_areas (
                    area_id, area_type, area_code, name, state_code, state_name,
                    parent_name, status, tracked_at, updated_at, last_error
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                ON CONFLICT(area_id) DO UPDATE SET
                    area_type = excluded.area_type,
                    area_code = excluded.area_code,
                    name = excluded.name,
                    state_code = excluded.state_code,
                    state_name = excluded.state_name,
                    parent_name = excluded.parent_name,
                    status = excluded.status,
                    updated_at = excluded.updated_at,
                    last_error = NULL
                """,
                (
                    area.area_id,
                    area.area_type,
                    area.area_code,
                    area.name,
                    area.state_code,
                    area.state_name,
                    area.parent_name,
                    status,
                    tracked_at,
                    now,
                ),
            )
            self._connection.commit()

    def remove_tracked(self, area_id: str) -> None:
        with self._lock:
            self._connection.execute("DELETE FROM tracked_areas WHERE area_id = ?", (area_id,))
            self._connection.commit()

    def set_status(
        self,
        area_id: str,
        status: str,
        *,
        last_error: str | None = None,
        latest_source_timestamp: str | None = None,
        media_path: str | None = None,
        snippet_path: str | None = None,
    ) -> None:
        updates = {
            "status": status,
            "updated_at": self._now(),
            "last_error": last_error,
        }
        if latest_source_timestamp is not None:
            updates["latest_source_timestamp"] = latest_source_timestamp
        if media_path is not None:
            updates["media_path"] = media_path
        if snippet_path is not None:
            updates["snippet_path"] = snippet_path
        assignments = ", ".join(f"{column} = :{column}" for column in updates)
        updates["area_id"] = area_id
        with self._lock:
            self._connection.execute(
                f"UPDATE tracked_areas SET {assignments} WHERE area_id = :area_id",
                updates,
            )
            self._connection.commit()

    @staticmethod
    def _row_to_tracked(row: sqlite3.Row) -> TrackedArea:
        return TrackedArea(
            area_id=str(row["area_id"]),
            area_type=str(row["area_type"]),
            area_code=str(row["area_code"]),
            name=str(row["name"]),
            state_code=str(row["state_code"]) if row["state_code"] else None,
            state_name=str(row["state_name"]) if row["state_name"] else None,
            parent_name=str(row["parent_name"]) if row["parent_name"] else None,
            status=str(row["status"]),
            tracked_at=str(row["tracked_at"]),
            updated_at=str(row["updated_at"]),
            last_error=row["last_error"],
            latest_source_timestamp=row["latest_source_timestamp"],
            media_path=row["media_path"],
            snippet_path=row["snippet_path"],
        )

    @staticmethod
    def _now() -> str:
        return datetime.now(UTC).isoformat()
