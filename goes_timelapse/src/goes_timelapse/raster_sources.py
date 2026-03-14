from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import numpy as np
import rasterio
from PIL import Image
from rasterio import Affine
from rasterio.crs import CRS
from rasterio.enums import Resampling
from rasterio.transform import from_bounds
from rasterio.warp import reproject
from rasterio.vrt import WarpedVRT
from rasterio.windows import transform as window_transform
from rasterio.windows import Window


WGS84_CRS = CRS.from_epsg(4326)
NETCDF_WGS84_OVERSAMPLE = 2


class RasterSource(Protocol):
    width: int
    height: int
    transform: Affine
    crs: CRS

    def read_image(
        self,
        window: Window,
        *,
        output_size: tuple[int, int] | None = None,
        dst_bounds: tuple[float, float, float, float] | None = None,
    ) -> Image.Image:
        ...

    def close(self) -> None:
        ...


@dataclass(slots=True)
class GeoTiffRasterSource:
    _dataset: rasterio.DatasetReader
    _vrt: WarpedVRT | None = None

    def __post_init__(self) -> None:
        dataset_crs = self._dataset.crs
        if dataset_crs and dataset_crs != WGS84_CRS:
            self._vrt = WarpedVRT(self._dataset, crs=WGS84_CRS, resampling=Resampling.bilinear)

    @property
    def dataset(self):
        return self._vrt or self._dataset

    @property
    def width(self) -> int:
        return int(self.dataset.width)

    @property
    def height(self) -> int:
        return int(self.dataset.height)

    @property
    def transform(self) -> Affine:
        return self.dataset.transform

    @property
    def crs(self) -> CRS:
        return self.dataset.crs or WGS84_CRS

    def read_image(
        self,
        window: Window,
        *,
        output_size: tuple[int, int] | None = None,
        dst_bounds: tuple[float, float, float, float] | None = None,
    ) -> Image.Image:
        dataset = self.dataset
        if dataset.count >= 3 and all(dtype == "uint8" for dtype in dataset.dtypes[:3]):
            red = dataset.read(1, window=window)
            green = dataset.read(2, window=window)
            blue = dataset.read(3, window=window)
            if dataset.count >= 4:
                alpha = dataset.read(4, window=window)
            else:
                alpha = np.full(red.shape, 255, dtype=np.uint8)
            rgba = np.dstack((red, green, blue, alpha)).astype(np.uint8)
            return Image.fromarray(rgba, mode="RGBA")

        if dataset.count >= 2 and dataset.dtypes[0] == "uint8" and dataset.dtypes[1] == "uint8":
            grayscale = dataset.read(1, window=window)
            alpha = dataset.read(2, window=window)
            rgba = np.dstack((grayscale, grayscale, grayscale, alpha)).astype(np.uint8)
            return Image.fromarray(rgba, mode="RGBA")

        subset = dataset.read(1, window=window).astype(np.float32)
        if dataset.count >= 2:
            mask = dataset.read(2, window=window)
            subset = np.where(mask != 0, subset, np.nan)

        grayscale = _normalize_to_uint8(subset)
        return Image.fromarray(grayscale, mode="L").convert("RGBA")

    def close(self) -> None:
        if self._vrt is not None:
            self._vrt.close()
        self._dataset.close()


class GoesNetcdfRasterSource:
    def __init__(self, path: Path):
        source_path = str(path).replace("\\", "/")
        self._cmi_dataset = rasterio.open(f"NETCDF:{source_path}:CMI")
        self._dqf_dataset = rasterio.open(f"NETCDF:{source_path}:DQF")

    @property
    def width(self) -> int:
        return int(self._cmi_dataset.width)

    @property
    def height(self) -> int:
        return int(self._cmi_dataset.height)

    @property
    def transform(self) -> Affine:
        return self._cmi_dataset.transform

    @property
    def crs(self) -> CRS:
        return self._cmi_dataset.crs or WGS84_CRS

    def read_image(
        self,
        window: Window,
        *,
        output_size: tuple[int, int] | None = None,
        dst_bounds: tuple[float, float, float, float] | None = None,
    ) -> Image.Image:
        if output_size and dst_bounds:
            subset, dqf = self._read_reprojected_crop(window, output_size, dst_bounds)
        else:
            subset = self._cmi_dataset.read(1, window=window, boundless=True).astype(np.float32)
            dqf = self._dqf_dataset.read(1, window=window, boundless=True).astype(np.uint8)
        subset = np.where(dqf == 0, subset, np.nan)
        grayscale = _normalize_visible_cmi_to_uint8(subset)
        return Image.fromarray(grayscale, mode="L").convert("RGBA")

    def close(self) -> None:
        self._cmi_dataset.close()
        self._dqf_dataset.close()

    def _read_reprojected_crop(
        self,
        window: Window,
        output_size: tuple[int, int],
        dst_bounds: tuple[float, float, float, float],
    ) -> tuple[np.ndarray, np.ndarray]:
        src_transform = window_transform(window, self._cmi_dataset.transform)
        src_cmi = self._cmi_dataset.read(1, window=window, boundless=True).astype(np.float32)
        src_dqf = self._dqf_dataset.read(1, window=window, boundless=True).astype(np.uint8)

        output_width, output_height = output_size
        dense_width = output_width * NETCDF_WGS84_OVERSAMPLE
        dense_height = output_height * NETCDF_WGS84_OVERSAMPLE
        dst_transform = from_bounds(
            dst_bounds[0],
            dst_bounds[1],
            dst_bounds[2],
            dst_bounds[3],
            dense_width,
            dense_height,
        )

        dense_cmi = np.full((dense_height, dense_width), np.nan, dtype=np.float32)
        dense_dqf = np.full((dense_height, dense_width), 255, dtype=np.uint8)

        reproject(
            source=src_cmi,
            destination=dense_cmi,
            src_transform=src_transform,
            src_crs=self.crs,
            src_nodata=self._cmi_dataset.nodata,
            dst_transform=dst_transform,
            dst_crs=WGS84_CRS,
            dst_nodata=np.nan,
            resampling=Resampling.cubic,
        )
        reproject(
            source=src_dqf,
            destination=dense_dqf,
            src_transform=src_transform,
            src_crs=self.crs,
            dst_transform=dst_transform,
            dst_crs=WGS84_CRS,
            dst_nodata=255,
            resampling=Resampling.nearest,
        )

        dense_image = Image.fromarray(np.nan_to_num(dense_cmi, nan=0.0).astype(np.float32), mode="F")
        dense_valid = Image.fromarray(np.where(dense_dqf == 0, 255, 0).astype(np.uint8), mode="L")
        image = dense_image.resize((output_width, output_height), Image.Resampling.BICUBIC)
        mask = dense_valid.resize((output_width, output_height), Image.Resampling.NEAREST)
        subset = np.asarray(image, dtype=np.float32)
        dqf = np.where(np.asarray(mask, dtype=np.uint8) > 0, 0, 255).astype(np.uint8)
        subset = np.where(dqf == 0, subset, np.nan)
        return subset, dqf


def open_raster_source(path: Path) -> RasterSource:
    suffix = path.suffix.lower()
    if suffix == ".nc":
        return GoesNetcdfRasterSource(path)
    if suffix in {".tif", ".tiff"}:
        return GeoTiffRasterSource(rasterio.open(path))
    raise ValueError(f"Unsupported raw format: {path}")


def _normalize_to_uint8(subset: np.ndarray) -> np.ndarray:
    finite_mask = np.isfinite(subset)
    if not finite_mask.any():
        return np.zeros(subset.shape, dtype=np.uint8)

    values = subset[finite_mask].astype(np.float32)
    low = float(np.percentile(values, 2))
    high = float(np.percentile(values, 98))
    if high <= low:
        high = low + 1.0

    normalized = np.zeros(subset.shape, dtype=np.float32)
    normalized[finite_mask] = np.clip((subset[finite_mask] - low) / (high - low), 0.0, 1.0)
    return np.round(normalized * 255).astype(np.uint8)


def _normalize_visible_cmi_to_uint8(subset: np.ndarray) -> np.ndarray:
    finite_mask = np.isfinite(subset)
    if not finite_mask.any():
        return np.zeros(subset.shape, dtype=np.uint8)

    values = subset[finite_mask].astype(np.float32)
    low = float(np.percentile(values, 0.5))
    high = float(np.percentile(values, 99.8))

    if high <= low:
        low = float(np.nanmin(values))
        high = float(np.nanmax(values))
    if high <= low:
        high = low + 1.0

    normalized = np.zeros(subset.shape, dtype=np.float32)
    normalized[finite_mask] = np.clip((subset[finite_mask] - low) / (high - low), 0.0, 1.0)

    # Keep more mid-tones and avoid the harsh black/white look from aggressive stretching.
    normalized = np.power(normalized, 0.92, dtype=np.float32)
    normalized = 0.06 + (normalized * 0.88)
    normalized[~finite_mask] = 0.0
    return np.round(np.clip(normalized, 0.0, 1.0) * 255).astype(np.uint8)
