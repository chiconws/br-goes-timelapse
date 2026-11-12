"""Tests for the municipality catalog."""

from __future__ import annotations

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
