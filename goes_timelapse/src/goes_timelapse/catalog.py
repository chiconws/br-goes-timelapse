from __future__ import annotations

import gzip
import json
import re
import unicodedata
from pathlib import Path

from goes_timelapse.models import AreaCatalogEntry, BoundaryLine


SUPPORTED_AREA_TYPES = frozenset({"municipio"})


AREA_TYPE_PRIORITY = {
    "municipio": 0,
}


def normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    normalized = re.sub(r"[^0-9a-z]+", " ", normalized.lower())
    normalized = " ".join(normalized.split())
    return normalized


def load_boundary_lines(path: Path) -> tuple[BoundaryLine, ...]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        raw_data = json.load(handle)
    return tuple(
        BoundaryLine(
            bounds=tuple(float(value) for value in item["bounds"]),
            line=tuple((float(lon), float(lat)) for lon, lat in item["line"]),
        )
        for item in raw_data
    )


class AreaCatalog:
    def __init__(self, areas: list[AreaCatalogEntry]):
        filtered_areas = [area for area in areas if area.area_type in SUPPORTED_AREA_TYPES]
        self._areas = sorted(
            filtered_areas,
            key=lambda area: (
                AREA_TYPE_PRIORITY.get(area.area_type, 99),
                -(area.population or -1),
                area.name,
                area.state_code or "",
            ),
        )
        self._by_id = {area.area_id: area for area in filtered_areas}

    @classmethod
    def from_path(cls, path: Path) -> "AreaCatalog":
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            raw_data = json.load(handle)
        areas = [
            AreaCatalogEntry(
                area_id=str(item["area_id"]),
                area_type=str(item["area_type"]),
                area_code=str(item["area_code"]),
                name=str(item["name"]),
                search_text=str(item["search_text"]),
                population=int(item["population"]) if item.get("population") is not None else None,
                state_code=str(item["state_code"]) if item.get("state_code") else None,
                state_name=str(item["state_name"]) if item.get("state_name") else None,
                parent_code=str(item["parent_code"]) if item.get("parent_code") else None,
                parent_name=str(item["parent_name"]) if item.get("parent_name") else None,
            )
            for item in raw_data
        ]
        return cls(areas)

    def get(self, area_id: str) -> AreaCatalogEntry | None:
        return self._by_id.get(area_id)

    def search(self, query: str, limit: int = 20) -> list[AreaCatalogEntry]:
        cleaned_query = normalize_text(query)
        if not cleaned_query:
            return self._areas[:limit]

        query_tokens = tuple(cleaned_query.split())
        scored: list[tuple[tuple[int, int, int, int], AreaCatalogEntry]] = []
        for area in self._areas:
            score = self._score_area(area, cleaned_query, query_tokens)
            if score is None:
                continue
            scored.append((score, area))

        scored.sort(
            key=lambda item: (
                *item[0],
                AREA_TYPE_PRIORITY.get(item[1].area_type, 99),
                -(item[1].population or -1),
                item[1].name,
                item[1].state_code or "",
            )
        )
        return [area for _, area in scored[:limit]]

    def _score_area(
        self,
        area: AreaCatalogEntry,
        cleaned_query: str,
        query_tokens: tuple[str, ...],
    ) -> tuple[int, int, int, int] | None:
        code = area.area_code.lower()
        normalized_name = normalize_text(area.name)
        normalized_display_name = normalize_text(area.display_name)
        search_text = area.search_text

        if code == cleaned_query:
            return (0, 0, 0, 0)
        if code.startswith(cleaned_query):
            return (1, 0, len(code) - len(cleaned_query), 0)

        exact_name_score = self._score_exact_match(normalized_name, normalized_display_name, cleaned_query)
        if exact_name_score is not None:
            return exact_name_score

        prefix_score = self._score_prefix_match(
            normalized_name,
            normalized_display_name,
            cleaned_query,
        )
        if prefix_score is not None:
            return prefix_score

        token_score = self._score_token_match(
            normalized_name,
            normalized_display_name,
            query_tokens,
        )
        if token_score is not None:
            return token_score

        if search_text.startswith(cleaned_query):
            return (6, 0, 0, 0)
        if cleaned_query in search_text:
            return (7, 0, 0, 0)
        return None

    @staticmethod
    def _score_exact_match(
        normalized_name: str,
        normalized_display_name: str,
        cleaned_query: str,
    ) -> tuple[int, int, int, int] | None:
        if normalized_name == cleaned_query:
            return (2, 0, 0, 0)
        if normalized_display_name == cleaned_query:
            return (2, 1, 0, 0)
        return None

    @staticmethod
    def _score_prefix_match(
        normalized_name: str,
        normalized_display_name: str,
        cleaned_query: str,
    ) -> tuple[int, int, int, int] | None:
        if normalized_name.startswith(cleaned_query):
            return (3, 0, len(normalized_name) - len(cleaned_query), 0)
        if normalized_display_name.startswith(cleaned_query):
            return (3, 1, len(normalized_display_name) - len(cleaned_query), 0)
        return None

    @staticmethod
    def _score_token_match(
        normalized_name: str,
        normalized_display_name: str,
        query_tokens: tuple[str, ...],
    ) -> tuple[int, int, int, int] | None:
        if not query_tokens:
            return None

        name_tokens = tuple(normalized_name.split())
        display_tokens = tuple(normalized_display_name.split())

        name_score = AreaCatalog._token_match_score(name_tokens, query_tokens)
        display_score = AreaCatalog._token_match_score(display_tokens, query_tokens)
        if name_score is not None:
            return (4, 0, *name_score)
        if display_score is not None:
            return (4, 1, *display_score)
        return None

    @staticmethod
    def _token_match_score(
        haystack_tokens: tuple[str, ...],
        query_tokens: tuple[str, ...],
    ) -> tuple[int, int] | None:
        if not haystack_tokens:
            return None

        if len(query_tokens) == 1:
            query_token = query_tokens[0]
            for index, token in enumerate(haystack_tokens):
                if token == query_token:
                    return (index, len(token) - len(query_token))
            for index, token in enumerate(haystack_tokens):
                if token.startswith(query_token):
                    return (index, len(token) - len(query_token))
            return None

        token_positions: list[int] = []
        remainder = 0
        start_index = 0
        for query_token in query_tokens:
            matched = False
            for index in range(start_index, len(haystack_tokens)):
                token = haystack_tokens[index]
                if token.startswith(query_token):
                    token_positions.append(index)
                    remainder += len(token) - len(query_token)
                    start_index = index + 1
                    matched = True
                    break
            if not matched:
                return None
        return (token_positions[0], remainder)
