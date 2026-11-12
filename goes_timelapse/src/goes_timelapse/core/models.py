"""Domain models for municipalities, geometry and tracked areas."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

AREA_TYPE_LABELS = {
    "municipio": "Município",
}


@dataclass(slots=True, frozen=True)
class BoundaryLine:
    """Polyline of a state boundary drawn over frames."""

    bounds: tuple[float, float, float, float]
    line: tuple[tuple[float, float], ...]


@dataclass(slots=True, frozen=True)
class AreaCatalogEntry:
    """Municipality from the bundled catalog."""

    area_id: str
    area_type: str
    area_code: str
    name: str
    search_text: str
    population: int | None
    state_code: str | None
    state_name: str | None
    parent_code: str | None
    parent_name: str | None

    @property
    def type_label(self) -> str:
        """Human label for the area type."""
        return AREA_TYPE_LABELS.get(self.area_type, self.area_type)

    @property
    def context_label(self) -> str | None:
        """State shown next to the name, if any."""
        if self.area_type == "municipio":
            return self.state_code
        return None

    @property
    def display_name(self) -> str:
        """Name with state, as shown in the UI."""
        if self.context_label:
            return f"{self.name} - {self.context_label}"
        return self.name

    @property
    def code_label(self) -> str:
        """IBGE code label."""
        if self.area_type == "municipio":
            return f"IBGE {self.area_code}"
        return f"Código {self.area_code}"


@dataclass(slots=True, frozen=True)
class AreaGeometry:
    """Municipality outline with bounds and centroid."""

    area_id: str
    centroid: tuple[float, float]
    bounds: tuple[float, float, float, float]
    polygon: tuple[tuple[float, float], ...]


@dataclass(slots=True, frozen=True)
class TrackedArea:
    """Municipality being tracked, with status and optional marker."""

    area_id: str
    area_type: str
    area_code: str
    name: str
    state_code: str | None
    state_name: str | None
    parent_name: str | None
    status: str
    tracked_at: str
    updated_at: str
    last_error: str | None
    latest_source_timestamp: str | None
    media_path: str | None
    snippet_path: str | None
    marker_lat: float | None
    marker_lon: float | None

    @property
    def type_label(self) -> str:
        """Human label for the area type."""
        return AREA_TYPE_LABELS.get(self.area_type, self.area_type)

    @property
    def context_label(self) -> str | None:
        """State shown next to the name, if any."""
        if self.area_type == "municipio":
            return self.state_code
        return None

    @property
    def display_name(self) -> str:
        """Name with state, as shown in the UI."""
        if self.context_label:
            return f"{self.name} - {self.context_label}"
        return self.name

    @property
    def code_label(self) -> str:
        """IBGE code label."""
        if self.area_type == "municipio":
            return f"IBGE {self.area_code}"
        return f"Código {self.area_code}"


@dataclass(slots=True, frozen=True)
class RenderedArea:
    """Result of rendering one municipality."""

    area: AreaCatalogEntry
    png_paths: list[Path]
    media_path: Path
    snippet_path: Path
    latest_source_timestamp: str
