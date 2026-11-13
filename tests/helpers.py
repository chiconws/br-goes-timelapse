"""Writers for synthetic GeoTIFF test files."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import rasterio
from rasterio import Affine


def create_test_tif(
    path: Path,
    transform: Affine | None = None,
    *,
    width: int = 512,
    height: int = 512,
    offset: float = 0.0,
) -> Path:
    """Write a small gray+alpha GeoTIFF like the cached crops."""
    transform = transform or Affine(0.02, 0.0, -50.0, 0.0, -0.02, -20.0)
    band = np.linspace(
        0 + offset, 200 + offset, width * height, dtype=np.float32
    ).reshape(height, width)
    hotspot_offset = int(offset) % max(1, min(width, height) // 4)
    row_start = 40 + hotspot_offset
    row_end = min(row_start + 48, height)
    col_start = 60 + hotspot_offset
    col_end = min(col_start + 48, width)
    band[row_start:row_end, col_start:col_end] += 50
    mask = np.full((height, width), 255, dtype=np.uint8)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=width,
        height=height,
        count=2,
        dtype="uint8",
        crs="EPSG:4326",
        transform=transform,
    ) as dst:
        dst.write(np.clip(band, 0, 255).astype(np.uint8), 1)
        dst.write(mask, 2)
    return path
