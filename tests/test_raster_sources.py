"""Tests for raster sources."""

from __future__ import annotations

import numpy as np
import rasterio
from rasterio import Affine
from rasterio.windows import Window

from goes_timelapse.pipeline.raster_sources import open_raster_source
from tests.helpers import create_test_netcdf


def _window(col_off: int, row_off: int, width: int, height: int) -> Window:
    return Window.from_slices((row_off, row_off + height), (col_off, col_off + width))


def test_netcdf_source_reprojects_only_the_requested_crop(tmp_path):
    """Test netcdf source reprojects only the requested crop."""
    raw_path = create_test_netcdf(tmp_path / "sample.nc", width=256, height=256)

    source = open_raster_source(raw_path)
    try:
        image = source.read_image(
            _window(32, 32, 96, 96),
            output_size=(320, 320),
            dst_bounds=(-41.0, -21.0, -39.0, -19.0),
        )
        assert image.size == (320, 320)
    finally:
        source.close()


def test_geotiff_source_honors_requested_destination_bounds(tmp_path):
    """Test geotiff source honors requested destination bounds."""
    raw_path = tmp_path / "sample.tif"
    width = 40
    height = 40
    transform = Affine(1.0, 0.0, -60.0, 0.0, -1.0, -10.0)
    grayscale = np.zeros((height, width), dtype=np.uint8)
    grayscale[:, 20:] = 255
    alpha = np.full((height, width), 255, dtype=np.uint8)

    with rasterio.open(
        raw_path,
        "w",
        driver="GTiff",
        width=width,
        height=height,
        count=2,
        dtype="uint8",
        crs="EPSG:4326",
        transform=transform,
    ) as dst:
        dst.write(grayscale, 1)
        dst.write(alpha, 2)

    source = open_raster_source(raw_path)
    try:
        image = source.read_image(
            _window(10, 0, 20, 30),
            output_size=(200, 200),
            dst_bounds=(-38.0, -28.0, -30.0, -12.0),
        )
        image_array = np.asarray(image)
        assert image.size == (200, 200)
        assert float(image_array[..., 0].mean()) > 240
    finally:
        source.close()
