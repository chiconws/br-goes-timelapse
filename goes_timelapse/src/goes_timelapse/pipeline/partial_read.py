"""HTTP Range reads of the parts of NOAA netCDF files that are needed."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import http.client
import io
import logging
import math
import os
from pathlib import Path
import struct
import threading
import time
from typing import Any, cast
from urllib.parse import urlsplit
import zlib

import h5py
import numpy as np
import rasterio
from rasterio import Affine
from rasterio.crs import CRS
from rasterio.enums import Resampling
from rasterio.warp import reproject, transform_bounds
from typing_extensions import override

LOGGER = logging.getLogger(__name__)
WGS84_CRS = CRS.from_epsg(4326)

# Same output grid as Geo2Grid's wgs84_fit, which the renderer was tuned for.
GRID_RESOLUTION = 0.0057
# rendering._build_source_window asks for this margin; it stays transparent and is never read.
RENDER_MARGIN_PIXELS = 24
READ_MARGIN_PIXELS = 2
BLOCK_SIZE_BY_BAND = {"C02": 1 << 18, "C13": 1 << 16}
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
RANGE_RETRY_ATTEMPTS = 5
RANGE_TIMEOUT_SECONDS = 60
# Each request waits on a round trip of ~190 ms, so throughput comes from requests in flight.
READ_AHEAD_BLOCKS = 8

# Geo2Grid enhancement for C13 (satpy btemp_threshold) and C02 (crude 0-100%, gamma 2).
BTEMP_THRESHOLD = 242.0
BTEMP_MIN = 163.0
BTEMP_MAX = 330.0
BTEMP_THRESHOLD_OUT = 176 / 255

# Seen in every GLM-L2-LCFA file measured. Swapping these raises nothing, so
# GlmDownloader compares the direct read with read_glm_flashes_with_hdf5 every hour.
GLM_LAT_NODE = 139625
GLM_LON_NODE = 120761
GLM_NODE_SIZE = 2096
GLM_NODE_MAX_ENTRIES = 64
GLM_MERGE_GAP_BYTES = 4096
GLM_CHECK_BLOCK_SIZE = 1 << 14

BBox = tuple[float, float, float, float]


@dataclass(slots=True)
class CropStats:
    """Bytes, requests and timings of one partial read."""

    bytes_read: int = 0
    requests: int = 0
    retries: int = 0
    rows: int = 0
    tiff_bytes: int = 0
    network_seconds: float = 0.0
    decode_seconds: float = 0.0
    reproject_seconds: float = 0.0
    write_seconds: float = 0.0
    total_seconds: float = 0.0


class RangeFile(io.RawIOBase):
    """Read-only file over HTTP Range for h5py, counting what crosses the wire."""

    def __init__(self, url: str, *, block_size: int, user_agent: str):
        """Prepare connections for url, reading block_size bytes at a time."""
        self._parts = urlsplit(url)
        self._block_size = block_size
        self._headers = {"User-Agent": user_agent, "Accept-Encoding": "identity"}
        self._local = threading.local()
        self._connections: set[http.client.HTTPConnection] = set()
        self._lock = threading.Lock()
        self._blocks: dict[int, bytes] = {}
        self._position = 0
        self._last_read_block = -2
        self._read_ahead = 1
        self._pool = ThreadPoolExecutor(READ_AHEAD_BLOCKS)
        self.bytes_read = 0
        self.requests = 0
        self.retries = 0
        self.network_seconds = 0.0
        self.size = int(self._request("HEAD", {}, 200)[1])

    def _connect(self) -> http.client.HTTPConnection:
        connection = getattr(self._local, "connection", None)
        if connection is None:
            connection_type = (
                http.client.HTTPSConnection
                if self._parts.scheme == "https"
                else http.client.HTTPConnection
            )
            connection = connection_type(
                self._parts.netloc, timeout=RANGE_TIMEOUT_SECONDS
            )
            self._local.connection = connection
            with self._lock:
                self._connections.add(connection)
        return connection

    def _request(
        self, method: str, headers: dict[str, str], expected: int
    ) -> tuple[bytes, str]:
        for attempt in range(1, RANGE_RETRY_ATTEMPTS + 1):
            try:
                connection = self._connect()
                connection.request(
                    method, self._parts.path, headers={**self._headers, **headers}
                )
                response = connection.getresponse()
                body = response.read()
                with self._lock:
                    self.requests += 1
                if response.status == expected:
                    return body, response.headers.get("Content-Length", "0")
                if response.status not in RETRYABLE_STATUS:
                    raise OSError(f"HTTP {response.status} em {self._parts.path}")
                failure: Exception = OSError(f"HTTP {response.status}")
            except (ConnectionError, TimeoutError, http.client.HTTPException) as err:
                failure = err
                self._close_connection()
            if attempt == RANGE_RETRY_ATTEMPTS:
                raise failure
            with self._lock:
                self.retries += 1
            LOGGER.warning(
                "Retrying %s %s: %r", method, headers.get("Range", ""), failure
            )
            time.sleep(attempt)
        raise AssertionError("unreachable")

    def _close_connection(self) -> None:
        connection = getattr(self._local, "connection", None)
        if connection is not None:
            connection.close()
            self._local.connection = None
            with self._lock:
                self._connections.discard(connection)

    @override
    def close(self) -> None:
        """Close every pooled connection."""
        self._pool.shutdown()
        for connection in self._connections:
            connection.close()
        self._connections.clear()
        super().close()

    @override
    def readable(self) -> bool:
        """Return True: the file can be read."""
        return True

    @override
    def seekable(self) -> bool:
        """Return True: the file can seek."""
        return True

    @override
    def tell(self) -> int:
        """Return the current position."""
        return self._position

    @override
    def seek(self, offset: int, whence: int = 0) -> int:
        """Move the position like io.RawIOBase.seek."""
        self._position = {0: offset, 1: self._position + offset, 2: self.size + offset}[
            whence
        ]
        return self._position

    def _fetch_block(self, index: int) -> None:
        start = index * self._block_size
        end = min(start + self._block_size, self.size) - 1
        data, _ = self._request("GET", {"Range": f"bytes={start}-{end}"}, 206)
        if len(data) != end - start + 1:
            raise OSError(f"Leitura parcial curta em {self._parts.path}")
        with self._lock:
            self.bytes_read += len(data)
        self._blocks[index] = data

    def _block(self, index: int) -> bytes:
        if index not in self._blocks:
            # Like an OS read-ahead: index lookups jump around and stay single, chunk data runs
            # in file order and ramps up, so only sequential reads pay for the blocks they overrun.
            sequential = index == self._last_read_block + 1
            self._read_ahead = (
                min(self._read_ahead * 2, READ_AHEAD_BLOCKS) if sequential else 1
            )
            last_block = (self.size - 1) // self._block_size
            stop = min(index + self._read_ahead, last_block + 1)
            self._last_read_block = stop - 1
            started_at = time.perf_counter()
            list(self._pool.map(self._fetch_block, range(index, stop)))
            self.network_seconds += time.perf_counter() - started_at
        return self._blocks[index]

    @override
    def readinto(self, buffer: Any) -> int:  # writable buffer from h5py
        """Fill buffer from the current position."""
        count = min(len(buffer), max(self.size - self._position, 0))
        done = 0
        while done < count:
            index, offset = divmod(self._position + done, self._block_size)
            chunk = self._block(index)[offset : offset + count - done]
            buffer[done : done + len(chunk)] = chunk
            done += len(chunk)
        self._position += count
        return count


def write_abi_crop(
    url: str,
    band: str,
    output_path: Path,
    bbox: BBox,
    *,
    user_agent: str,
) -> CropStats:
    """Read only the rows of `bbox` and publish a gray+alpha WGS84 GeoTIFF."""
    started_at = time.perf_counter()
    stats = CropStats()
    left, bottom, right, top = bbox
    col_min = math.floor(left / GRID_RESOLUTION) - RENDER_MARGIN_PIXELS
    col_max = math.ceil(right / GRID_RESOLUTION) + RENDER_MARGIN_PIXELS
    row_min = -math.ceil(top / GRID_RESOLUTION) - RENDER_MARGIN_PIXELS
    row_max = -math.floor(bottom / GRID_RESOLUTION) + RENDER_MARGIN_PIXELS
    gray = np.zeros((row_max - row_min, col_max - col_min), dtype=np.uint8)
    alpha = np.zeros_like(gray)
    transform = Affine(
        GRID_RESOLUTION,
        0,
        col_min * GRID_RESOLUTION,
        0,
        -GRID_RESOLUTION,
        -row_min * GRID_RESOLUTION,
    )

    remote = RangeFile(url, block_size=BLOCK_SIZE_BY_BAND[band], user_agent=user_agent)
    try:
        with h5py.File(remote, "r") as dataset:
            source_crs = _geostationary_crs(dataset)
            height = float(
                _attrs(dataset, "goes_imager_projection")["perspective_point_height"][0]
            )
            axes = _axis_scales(dataset, height)
            _paint_bbox(
                dataset, band, bbox, source_crs, axes, gray, alpha, transform, stats
            )
    finally:
        remote.close()
    stats.bytes_read = remote.bytes_read
    stats.requests = remote.requests
    stats.retries = remote.retries
    stats.network_seconds = remote.network_seconds
    stats.decode_seconds = time.perf_counter() - started_at - remote.network_seconds
    stats.decode_seconds -= stats.reproject_seconds

    write_started_at = time.perf_counter()
    _publish_tiff(output_path, gray, alpha, transform)
    stats.tiff_bytes = output_path.stat().st_size
    stats.write_seconds = time.perf_counter() - write_started_at
    stats.total_seconds = time.perf_counter() - started_at
    return stats


def _geostationary_crs(dataset: h5py.File) -> CRS:
    projection = _attrs(dataset, "goes_imager_projection")
    sweep = projection["sweep_angle_axis"]
    sweep = sweep.decode() if isinstance(sweep, bytes) else str(sweep)
    return CRS.from_proj4(
        f"+proj=geos +h={float(projection['perspective_point_height'][0])} "
        f"+lon_0={float(projection['longitude_of_projection_origin'][0])} +sweep={sweep} "
        f"+a={float(projection['semi_major_axis'][0])} "
        f"+b={float(projection['semi_minor_axis'][0])} +units=m +no_defs"
    )


def _axis_scales(
    dataset: h5py.File, height: float
) -> tuple[float, float, float, float]:
    x_attrs, y_attrs = _attrs(dataset, "x"), _attrs(dataset, "y")
    return (
        float(x_attrs["scale_factor"][0]) * height,
        float(x_attrs["add_offset"][0]) * height,
        float(y_attrs["scale_factor"][0]) * height,
        float(y_attrs["add_offset"][0]) * height,
    )


def _paint_bbox(
    dataset: h5py.File,
    band: str,
    bbox: BBox,
    source_crs: CRS,
    axes: tuple[float, float, float, float],
    gray: np.ndarray,
    alpha: np.ndarray,
    transform: Affine,
    stats: CropStats,
) -> None:
    x_scale, x_offset, y_scale, y_offset = axes
    pad = READ_MARGIN_PIXELS * GRID_RESOLUTION
    padded = (bbox[0] - pad, bbox[1] - pad, bbox[2] + pad, bbox[3] + pad)
    x_min, y_min, x_max, y_max = transform_bounds(
        WGS84_CRS, source_crs, *padded, densify_pts=21
    )
    radiance = _node(dataset, "Rad")
    rows, columns = radiance.shape
    # Index math stays in float until floor/ceil; the y axis runs downward.
    row_start = max(math.floor((y_max - y_offset) / y_scale) - READ_MARGIN_PIXELS, 0)
    row_stop = min(
        math.ceil((y_min - y_offset) / y_scale) + 1 + READ_MARGIN_PIXELS, rows
    )
    col_start = max(math.floor((x_min - x_offset) / x_scale) - READ_MARGIN_PIXELS, 0)
    col_stop = min(
        math.ceil((x_max - x_offset) / x_scale) + 1 + READ_MARGIN_PIXELS, columns
    )
    if row_start >= row_stop or col_start >= col_stop:
        raise ValueError(f"Área fora do disco do satélite: {bbox}")

    stats.rows = row_stop - row_start
    counts = radiance[row_start:row_stop, col_start:col_stop]
    source_gray, source_alpha = _counts_to_gray_alpha(dataset, band, counts)
    reproject_started_at = time.perf_counter()
    source_transform = Affine(
        x_scale,
        0,
        x_offset + (col_start - 0.5) * x_scale,
        0,
        y_scale,
        y_offset + (row_start - 0.5) * y_scale,
    )

    inverse = ~transform
    col_left, row_top = inverse * (bbox[0], bbox[3])
    col_right, row_bottom = inverse * (bbox[2], bbox[1])
    window = (
        slice(
            max(math.floor(row_top) - READ_MARGIN_PIXELS, 0),
            math.ceil(row_bottom) + READ_MARGIN_PIXELS,
        ),
        slice(
            max(math.floor(col_left) - READ_MARGIN_PIXELS, 0),
            math.ceil(col_right) + READ_MARGIN_PIXELS,
        ),
    )
    target_gray = np.zeros_like(gray[window])
    target_alpha = np.zeros_like(alpha[window])
    target_transform = transform * Affine.translation(window[1].start, window[0].start)
    for source, target in ((source_gray, target_gray), (source_alpha, target_alpha)):
        reproject(
            source=source,
            destination=target,
            src_transform=source_transform,
            src_crs=source_crs,
            dst_transform=target_transform,
            dst_crs=WGS84_CRS,
            resampling=Resampling.nearest,
        )
    stats.reproject_seconds = time.perf_counter() - reproject_started_at
    valid = target_alpha > 0
    gray[window][valid] = target_gray[valid]
    alpha[window][valid] = 255


def _counts_to_gray_alpha(
    dataset: h5py.File,
    band: str,
    counts: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    attrs = _attrs(dataset, "Rad")
    fill_value = attrs["_FillValue"][0]
    scale = float(attrs["scale_factor"][0])
    offset = float(attrs["add_offset"][0])
    valid = counts != fill_value
    radiance = counts.astype(np.float64) * scale + offset
    if band == "C13":
        # Same clip as satpy: negative radiance has no brightness temperature.
        radiance = np.maximum(radiance, math.ceil(-offset / scale) * scale + offset)
        normalized = _enhance_brightness_temperature(
            _brightness_temperature(
                radiance,
                *(
                    float(_node(dataset, name)[()])
                    for name in (
                        "planck_fk1",
                        "planck_fk2",
                        "planck_bc1",
                        "planck_bc2",
                    )
                ),
            )
        )
    else:
        normalized = np.sqrt(
            np.clip(radiance * float(_node(dataset, "kappa0")[()]), 0.0, 1.0)
        )
    gray = np.rint(np.clip(normalized, 0.0, 1.0) * 255).astype(np.uint8)
    gray[~valid] = 0
    return gray, valid.astype(np.uint8) * 255


def _brightness_temperature(
    radiance: np.ndarray,
    fk1: float,
    fk2: float,
    bc1: float,
    bc2: float,
) -> np.ndarray:
    return cast(np.ndarray, (fk2 / np.log(fk1 / radiance + 1.0) - bc1) / bc2)


def _enhance_brightness_temperature(temperature: np.ndarray) -> np.ndarray:
    high_factor = BTEMP_THRESHOLD_OUT / (BTEMP_MAX - BTEMP_THRESHOLD)
    low_factor = (BTEMP_THRESHOLD_OUT - 1.0) / (BTEMP_MIN - BTEMP_THRESHOLD)
    return np.where(
        temperature >= BTEMP_THRESHOLD,
        high_factor * (BTEMP_MAX - temperature),
        1.0 + low_factor * BTEMP_MIN - low_factor * temperature,
    )


def _publish_tiff(
    output_path: Path,
    gray: np.ndarray,
    alpha: np.ndarray,
    transform: Affine,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    publish_path = output_path.with_name(f".{output_path.name}.tmp")
    publish_path.unlink(missing_ok=True)
    try:
        with rasterio.open(
            publish_path,
            "w",
            driver="GTiff",
            width=gray.shape[1],
            height=gray.shape[0],
            count=2,
            dtype="uint8",
            crs=WGS84_CRS,
            transform=transform,
            tiled=True,
            blockxsize=256,
            blockysize=256,
            compress="lzw",
        ) as destination:
            destination.write(gray, 1)
            destination.write(alpha, 2)
        with publish_path.open("rb") as handle:
            os.fsync(handle.fileno())
        publish_path.replace(output_path)
        _fsync_directory(output_path.parent)
    except Exception:
        publish_path.unlink(missing_ok=True)
        raise


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        directory_fd = os.open(path, flags)
    except OSError:
        LOGGER.debug("Skipping directory fsync for %s", path, exc_info=True)
        return
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def parse_glm_node(node: bytes) -> list[tuple[int, int]]:
    """HDF5 B-tree v1 leaf of a 1-D chunked dataset: (address, stored size) per chunk."""
    if len(node) < GLM_NODE_SIZE or node[:4] != b"TREE" or node[4] != 1 or node[5] != 0:
        raise ValueError("Nó GLM fora do layout esperado")
    used = struct.unpack_from("<H", node, 6)[0]
    if not 1 <= used <= GLM_NODE_MAX_ENTRIES:
        raise ValueError(f"Nó GLM com {used} entradas")
    entries: list[tuple[int, int]] = []
    for index in range(used):
        position = 24 + index * 32
        size, filter_mask = struct.unpack_from("<II", node, position)
        if filter_mask != 0:
            raise ValueError("Chunk GLM com filtro desativado")
        entries.append((struct.unpack_from("<Q", node, position + 24)[0], size))
    return entries


def glm_read_ranges(chunks: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Merge chunks into (start, end-exclusive) reads; S3 takes one range per request."""
    ranges: list[list[int]] = []
    for address, size in sorted(chunks):
        if ranges and address <= ranges[-1][1] + GLM_MERGE_GAP_BYTES:
            ranges[-1][1] = max(ranges[-1][1], address + size)
        else:
            ranges.append([address, address + size])
    return [(start, end) for start, end in ranges]


def decode_glm_chunks(
    chunks: list[tuple[int, int]],
    ranges: dict[tuple[int, int], bytes],
) -> np.ndarray:
    """Decompress and join the HDF5 chunks of a GLM variable."""
    values: list[np.ndarray] = []
    for address, size in chunks:
        start, end = next(
            item for item in ranges if item[0] <= address and address + size <= item[1]
        )
        values.append(
            _decode_glm_chunk(
                ranges[(start, end)][address - start : address - start + size]
            )
        )
    return np.concatenate(values)


def _decode_glm_chunk(blob: bytes) -> np.ndarray:
    raw = np.frombuffer(zlib.decompress(blob), dtype=np.uint8)
    if raw.size % 4:
        raise ValueError("Chunk GLM com tamanho inválido")
    # HDF5 shuffle stores byte 0 of every value first, then byte 1, and so on.
    return np.frombuffer(raw.reshape(4, -1).T.tobytes(), dtype="<f4")


def read_glm_flashes_with_hdf5(url: str, *, user_agent: str) -> list[dict[str, float]]:
    """Let libhdf5 find the columns itself, which the fixed GLM_*_NODE offsets cannot prove."""
    remote = RangeFile(url, block_size=GLM_CHECK_BLOCK_SIZE, user_agent=user_agent)
    try:
        with h5py.File(remote, "r") as dataset:
            return valid_flash_points(
                _node(dataset, "flash_lat")[:], _node(dataset, "flash_lon")[:]
            )
    finally:
        remote.close()


def valid_flash_points(
    latitudes: Iterable[float], longitudes: Iterable[float]
) -> list[dict[str, float]]:
    """Return the finite flash coordinates as points."""
    return [
        {"lat": latitude, "lon": longitude}
        for latitude, longitude in zip(
            map(float, latitudes), map(float, longitudes), strict=False
        )
        if -90.0 <= latitude <= 90.0 and -180.0 <= longitude <= 180.0
    ]


def _node(dataset: h5py.File, name: str) -> h5py.Dataset:
    """Return a variable of the file; GOES files keep them at the root."""
    return cast(h5py.Dataset, dataset[name])


def _attrs(dataset: h5py.File, name: str) -> Mapping[str, Any]:
    """Return the attributes of a variable of the file."""
    return cast(Mapping[str, Any], dataset[name].attrs)
