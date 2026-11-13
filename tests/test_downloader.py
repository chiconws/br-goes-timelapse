"""Tests for the NOAA listing parsers and retry policy."""

import aiohttp

from goes_timelapse.pipeline.downloader import (
    DOWNLOAD_RETRYABLE_ERRORS,
    GlmDownloader,
    GoesDownloader,
)


def _build_downloader(tmp_path):
    return GoesDownloader(
        base_url="https://example.invalid/",
        raw_dir=tmp_path / "raw",
    )


def _build_glm_downloader(tmp_path):
    return GlmDownloader(
        base_url="https://example.invalid/",
        raw_dir=tmp_path / "raw",
    )


def test_parse_listing_sorts_newest_first_and_deduplicates(tmp_path):
    """Test parse listing sorts newest first and deduplicates."""
    xml_payload = """
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Contents>
        <Key>ABI-L1b-RadF/2026/001/12/OR_ABI-L1b-RadF-M6C02_G19_s20260011202149_e20260011209522_c20260011209568.nc</Key>
      </Contents>
      <Contents>
        <Key>ABI-L1b-RadF/2026/001/11/OR_ABI-L1b-RadF-M6C02_G19_s20260011152149_e20260011159522_c20260011159568.nc</Key>
      </Contents>
      <Contents>
        <Key>ABI-L1b-RadF/2026/001/12/OR_ABI-L1b-RadF-M6C02_G19_s20260011202149_e20260011209522_c20260011209568.nc</Key>
      </Contents>
    </ListBucketResult>
    """

    result = _build_downloader(tmp_path).parse_listing(xml_payload)

    assert result == [
        "OR_ABI-L1b-RadF-M6C02_G19_s20260011202149_e20260011209522_c20260011209568.nc",
        "OR_ABI-L1b-RadF-M6C02_G19_s20260011152149_e20260011159522_c20260011159568.nc",
    ]


def test_parse_listing_filters_by_band_token(tmp_path):
    """Test parse listing filters by band token."""
    downloader = GoesDownloader(
        base_url="https://example.invalid/",
        raw_dir=tmp_path / "raw",
        band="C13",
    )
    xml_payload = """
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Contents>
        <Key>ABI-L1b-RadF/2026/001/12/OR_ABI-L1b-RadF-M6C13_G19_s20260011202149_e20260011209522_c20260011209568.nc</Key>
      </Contents>
      <Contents>
        <Key>ABI-L1b-RadF/2026/001/12/OR_ABI-L1b-RadF-M6C02_G19_s20260011202149_e20260011209522_c20260011209568.nc</Key>
      </Contents>
    </ListBucketResult>
    """

    assert downloader.parse_listing(xml_payload) == [
        "OR_ABI-L1b-RadF-M6C13_G19_s20260011202149_e20260011209522_c20260011209568.nc"
    ]


def test_retryable_download_errors_include_truncated_payload_case():
    """Test retryable download errors include truncated payload case."""
    assert aiohttp.ClientPayloadError in DOWNLOAD_RETRYABLE_ERRORS


def test_glm_parse_listing_sorts_newest_first_and_deduplicates(tmp_path):
    """Test glm parse listing sorts newest first and deduplicates."""
    xml_payload = """
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Contents>
        <Key>GLM-L2-LCFA/2026/086/12/OR_GLM-L2-LCFA_G19_s20260861200200_e20260861200400_c20260861200420.nc</Key>
      </Contents>
      <Contents>
        <Key>GLM-L2-LCFA/2026/086/12/OR_GLM-L2-LCFA_G19_s20260861140200_e20260861140400_c20260861140420.nc</Key>
      </Contents>
      <Contents>
        <Key>GLM-L2-LCFA/2026/086/12/OR_GLM-L2-LCFA_G19_s20260861200200_e20260861200400_c20260861200420.nc</Key>
      </Contents>
    </ListBucketResult>
    """

    result = _build_glm_downloader(tmp_path).parse_listing(xml_payload)

    assert result == [
        "OR_GLM-L2-LCFA_G19_s20260861200200_e20260861200400_c20260861200420.nc",
        "OR_GLM-L2-LCFA_G19_s20260861140200_e20260861140400_c20260861140420.nc",
    ]
