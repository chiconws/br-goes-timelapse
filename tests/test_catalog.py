"""Tests for the municipality catalog."""

from __future__ import annotations

from goes_timelapse.core.models import AreaCatalogEntry
from goes_timelapse.data.catalog import AreaCatalog, normalize_text


def test_normalize_text_removes_accents():
    """Test normalize text removes accents."""
    assert normalize_text("São José dos Campos") == "sao jose dos campos"


def test_normalize_text_replaces_punctuation_with_spaces():
    """Test normalize text replaces punctuation with spaces."""
    assert normalize_text("Vitória - ES") == "vitoria es"


def test_catalog_search_matches_name_code_and_state(settings):
    """Test catalog search matches name code and state."""
    catalog = AreaCatalog.from_path(settings.catalog_path)

    by_name = catalog.search("sumare")
    by_code = catalog.search("3552403")
    by_state = catalog.search("campinas sp")
    by_display_name = catalog.search("Vitória - ES")

    assert by_name[0].area_id == "municipio-3552403"
    assert by_code[0].name == "Sumare"
    assert by_state[0].area_id == "municipio-3509502"
    assert by_display_name[0].area_id == "municipio-3205309"


def test_catalog_search_uses_population_as_tiebreaker_within_same_relevance_bucket(
    settings,
):
    """Test catalog search uses population as tiebreaker within same relevance bucket."""
    catalog = AreaCatalog.from_path(settings.catalog_path)

    matches = catalog.search("espirito santo")

    assert matches[0].area_id == "municipio-3205002"
    assert matches[1].area_id == "municipio-3205200"
    assert matches[2].area_id == "municipio-3205309"


def test_catalog_filters_out_unsupported_area_types():
    """Test catalog filters out unsupported area types."""
    catalog = AreaCatalog(
        [
            AreaCatalogEntry(
                area_id="municipio-3205309",
                area_type="municipio",
                area_code="3205309",
                name="Vitoria",
                search_text="municipio 3205309 vitoria es espirito santo",
                population=322869,
                state_code="ES",
                state_name="Espirito Santo",
                parent_code=None,
                parent_name=None,
            ),
            AreaCatalogEntry(
                area_id="regiao-imediata-320001",
                area_type="regiao-imediata",
                area_code="320001",
                name="Vitoria",
                search_text="regiao imediata 320001 vitoria es espirito santo",
                population=None,
                state_code="ES",
                state_name="Espirito Santo",
                parent_code="3201",
                parent_name="Vitoria",
            ),
        ]
    )

    matches = catalog.search("vitoria")

    assert [area.area_id for area in matches] == ["municipio-3205309"]
