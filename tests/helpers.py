"""Writers for synthetic GeoTIFF and netCDF test files."""

from __future__ import annotations

from pathlib import Path

import netCDF4
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
    """Write a small grayscale GeoTIFF."""
    transform = transform or Affine(0.02, 0.0, -50.0, 0.0, -0.02, -20.0)
    band = np.linspace(
        0 + offset, 1000 + offset, width * height, dtype=np.float32
    ).reshape(height, width)
    hotspot_offset = int(offset) % max(1, min(width, height) // 4)
    row_start = 40 + hotspot_offset
    row_end = min(row_start + 48, height)
    col_start = 60 + hotspot_offset
    col_end = min(col_start + 48, width)
    band[row_start:row_end, col_start:col_end] += 400
    mask = np.ones((height, width), dtype=np.float32)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=width,
        height=height,
        count=2,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
    ) as dst:
        dst.write(band, 1)
        dst.write(mask, 2)
    return path


def create_test_rgba_tif(
    path: Path,
    transform: Affine | None = None,
    *,
    width: int = 512,
    height: int = 512,
) -> Path:
    """Write a small RGBA GeoTIFF."""
    transform = transform or Affine(0.02, 0.0, -50.0, 0.0, -0.02, -20.0)
    x = np.tile(np.linspace(0, 255, width, dtype=np.uint8), (height, 1))
    y = np.tile(
        np.linspace(255, 0, height, dtype=np.uint8).reshape(height, 1), (1, width)
    )
    red = x
    green = y
    blue = np.full((height, width), 96, dtype=np.uint8)
    alpha = np.full((height, width), 255, dtype=np.uint8)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=width,
        height=height,
        count=4,
        dtype="uint8",
        crs="EPSG:4326",
        transform=transform,
    ) as dst:
        dst.write(red, 1)
        dst.write(green, 2)
        dst.write(blue, 3)
        dst.write(alpha, 4)
    return path


def create_test_netcdf(
    path: Path,
    *,
    width: int = 512,
    height: int = 512,
    offset: float = 0.0,
    data_variable: str = "CMI",
) -> Path:
    """Write a small GOES ABI-like netCDF file."""
    x_values = np.linspace(-0.101332, 0.101332, width, dtype=np.float32)
    y_values = np.linspace(0.101332, -0.101332, height, dtype=np.float32)
    band = np.linspace(
        0 + offset, 1 + offset, width * height, dtype=np.float32
    ).reshape(height, width)
    hotspot_offset = int(offset) % max(1, min(width, height) // 4)
    row_start = 40 + hotspot_offset
    row_end = min(row_start + 48, height)
    col_start = 60 + hotspot_offset
    col_end = min(col_start + 48, width)
    band[row_start:row_end, col_start:col_end] += 0.2
    dqf = np.zeros((height, width), dtype=np.uint8)

    with netCDF4.Dataset(path, "w") as dataset:
        dataset.Conventions = "CF-1.8"
        dataset.createDimension("y", height)
        dataset.createDimension("x", width)

        x_var = dataset.createVariable("x", "f4", ("x",))
        y_var = dataset.createVariable("y", "f4", ("y",))
        x_var[:] = x_values
        y_var[:] = y_values
        x_var.standard_name = "projection_x_coordinate"
        x_var.units = "rad"
        y_var.standard_name = "projection_y_coordinate"
        y_var.units = "rad"

        projection = dataset.createVariable("goes_imager_projection", "i4")
        projection.grid_mapping_name = "geostationary"
        projection.perspective_point_height = 35786023.0
        projection.longitude_of_projection_origin = -75.0
        projection.semi_major_axis = 6378137.0
        projection.semi_minor_axis = 6356752.31414
        projection.sweep_angle_axis = "x"

        cmi = dataset.createVariable(data_variable, "f4", ("y", "x"), fill_value=-999.0)
        cmi[:] = band
        cmi.grid_mapping = "goes_imager_projection"
        cmi.coordinates = "y x"
        dqf_var = dataset.createVariable("DQF", "u1", ("y", "x"))
        dqf_var[:] = dqf
        dqf_var.grid_mapping = "goes_imager_projection"
        dqf_var.coordinates = "y x"

    return path


def create_test_glm_netcdf(
    path: Path,
    *,
    flashes: list[tuple[float, float]] | None = None,
) -> Path:
    """Write a small GLM-like netCDF file."""
    flashes = flashes or [(-40.3, -20.3), (-40.25, -20.28)]
    path.parent.mkdir(parents=True, exist_ok=True)

    with netCDF4.Dataset(path, "w") as dataset:
        dataset.createDimension("flash", len(flashes))

        flash_lat = dataset.createVariable("flash_lat", "f4", ("flash",))
        flash_lon = dataset.createVariable("flash_lon", "f4", ("flash",))

        flash_lat[:] = [latitude for _, latitude in flashes]
        flash_lon[:] = [longitude for longitude, _ in flashes]

    return path
