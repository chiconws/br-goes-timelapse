"""Tests for reading cached crops."""

from __future__ import annotations

import numpy as np
import rasterio
from rasterio import Affine
from rasterio.windows import Window

from goes_timelapse.pipeline.rendering import RenderPlan, _read_crop


def _window(col_off: int, row_off: int, width: int, height: int) -> Window:
    return Window.from_slices((row_off, row_off + height), (col_off, col_off + width))


def test_geotiff_source_honors_requested_destination_bounds(tmp_path):
    """Test geotiff source honors requested destination bounds."""
    raw_path = tmp_path / "sample.tif"
    width = 40
    height = 40
    transform = Affine(1.0, 0.0, -60.0, 0.0, -1.0, -10.0)
    grayscale = np.zeros((height, width), dtype=np.uint8)
    grayscale[:, 20:] = 255
    alpha = np.full((height, width), 255, dtype=np.uint8)
    alpha[10:20, 25:35] = 0

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

    image = _read_crop(
        raw_path,
        RenderPlan(
            window=_window(10, 0, 20, 30),
            dst_bounds=(-38.0, -28.0, -30.0, -12.0),
            scaled_polygon=(),
            scaled_state_lines=(),
            output_size=(200, 200),
        ),
    )
    image_array = np.asarray(image)
    assert image.size == (200, 200)
    assert float(image_array[..., 0].mean()) > 240
    assert image_array[..., 3].min() == 0
    assert image_array[..., 3].max() == 255
