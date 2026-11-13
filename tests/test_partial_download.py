"""Tests for HTTP Range reads of ABI and GLM files."""

from __future__ import annotations

from datetime import UTC, datetime
from functools import partial
import http.server
import json
from pathlib import Path
import struct
import threading
from types import SimpleNamespace

import aiohttp
import h5py
import numpy as np
import pytest
import rasterio

from goes_timelapse.core.state import StateStore
from goes_timelapse.data.catalog import AreaCatalog
from goes_timelapse.pipeline import partial_read
import goes_timelapse.pipeline.downloader as downloader_module
from goes_timelapse.pipeline.downloader import AreaCrop, GlmDownloader, GoesDownloader
from goes_timelapse.pipeline.partial_read import (
    _enhance_brightness_temperature,
    glm_read_ranges,
    parse_glm_node,
    valid_flash_points,
    write_abi_crop,
)
from goes_timelapse.pipeline.service import GoesTimelapseService

GLM_NAME = "OR_GLM-L2-LCFA_G19_s20260011200000_e20260011200200_c20260011200220.nc"
LAT_NODE, LON_NODE = 1 << 20, (1 << 20) + 4096


def _glm_payload(tmp_path: Path, count: int) -> bytes:
    """A real libhdf5 file, with leaf nodes planted where the reader looks."""
    path = tmp_path / "glm.nc"
    latitudes = np.linspace(-30.0, 5.0, count, dtype="<f4")
    longitudes = np.linspace(-70.0, -35.0, count, dtype="<f4")
    with h5py.File(path, "w") as dataset:
        for name, values in (("flash_lat", latitudes), ("flash_lon", longitudes)):
            dataset.create_dataset(
                name,
                data=values,
                chunks=(2048,),
                maxshape=(None,),
                fillvalue=9.96921e36,
                compression="gzip",
                shuffle=True,
            )
    payload = bytearray(path.read_bytes())
    payload.extend(b"\0" * (LON_NODE + 2096 - len(payload)))
    with h5py.File(path, "r") as dataset:
        for name, node_at in (
            ("flash_lat", LAT_NODE),
            ("flash_lon", LON_NODE),
        ):
            chunks: list = []
            dataset[name].id.chunk_iter(chunks.append)
            node = bytearray(
                b"TREE\x01\x00" + struct.pack("<H", len(chunks)) + b"\0" * 2088
            )
            for index, chunk in enumerate(chunks):
                position = 24 + index * 32
                struct.pack_into("<II", node, position, chunk.size, 0)
                struct.pack_into("<Q", node, position + 24, chunk.byte_offset)
            payload[node_at : node_at + 2096] = node
    path.write_bytes(bytes(payload))
    return bytes(payload)


class _FakeResponse:
    def __init__(self, status: int, body: bytes):
        self.status = status
        self._body = body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise aiohttp.ClientError(f"HTTP {self.status}")

    async def read(self) -> bytes:
        return self._body


class _FakeSession:
    def __init__(self, payload: bytes, *, failures: int = 0):
        self.payload = payload
        self.failures = failures

    def get(self, url, **kwargs):
        if self.failures:
            self.failures -= 1
            return _FakeResponse(503, b"")
        start, end = kwargs["headers"]["Range"].removeprefix("bytes=").split("-")
        return _FakeResponse(206, self.payload[int(start) : int(end) + 1])


def _glm_downloader(tmp_path: Path, monkeypatch) -> GlmDownloader:
    monkeypatch.setattr(downloader_module, "GLM_LAT_NODE", LAT_NODE)
    monkeypatch.setattr(downloader_module, "GLM_LON_NODE", LON_NODE)
    downloader = GlmDownloader(
        base_url="https://example.invalid/",
        raw_dir=tmp_path / "raw",
    )
    downloader._layout_checked_at = float("inf")
    return downloader


def _no_sleep(monkeypatch) -> None:
    async def immediately(_seconds):
        return None

    monkeypatch.setattr(downloader_module.asyncio, "sleep", immediately)


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [1, 700, 2048, 2049, 7000])
async def test_glm_direct_read_matches_whole_file(tmp_path, monkeypatch, count):
    """Test glm direct read matches whole file."""
    payload = _glm_payload(tmp_path, count)
    downloader = _glm_downloader(tmp_path, monkeypatch)

    flashes = await downloader._read_flash_columns(_FakeSession(payload), "key")

    with h5py.File(tmp_path / "glm.nc", "r") as dataset:
        expected = valid_flash_points(
            dataset["flash_lat"][()], dataset["flash_lon"][()]
        )
    assert flashes == expected
    assert len(flashes) == count


@pytest.mark.asyncio
async def test_glm_direct_read_retries_slow_down(tmp_path, monkeypatch):
    """Test glm direct read retries slow down."""
    _no_sleep(monkeypatch)
    downloader = _glm_downloader(tmp_path, monkeypatch)
    session = _FakeSession(_glm_payload(tmp_path, 50), failures=2)

    assert len(await downloader._read_flash_columns(session, "key")) == 50


def test_glm_node_rejects_inner_btree_level():
    """Test glm node rejects inner btree level."""
    node = b"TREE\x01\x01\x01\x00" + b"\0" * 2088
    with pytest.raises(ValueError):
        parse_glm_node(node)


def test_glm_read_ranges_merges_only_close_chunks():
    """Test glm read ranges merges only close chunks."""
    assert glm_read_ranges([(9000, 100), (1000, 100), (1200, 50)]) == [
        (1000, 1250),
        (9000, 9100),
    ]
    assert glm_read_ranges([(1000, 100), (6000, 100)]) == [(1000, 1100), (6000, 6100)]


def test_brightness_temperature_enhancement_matches_geo2grid_output():
    """Test brightness temperature enhancement matches geo2grid output."""
    # Medians measured on a real Geo2Grid C13 GeoTIFF.
    values = _enhance_brightness_temperature(
        np.array([244.41868980904596, 260.90189100513027])
    )
    assert np.rint(values * 255).tolist() == [171.0, 138.0]


def _write_fake_abi(path: Path, band: str, size: int = 2712) -> None:
    radiance = np.random.default_rng(1).integers(100, 1100, (size, size), dtype="i2")
    radiance[0:5, :] = 4095
    with h5py.File(path, "w") as dataset:
        rad = dataset.create_dataset(
            "Rad", data=radiance, chunks=(24, size), compression="gzip", shuffle=True
        )
        rad.attrs["_FillValue"] = np.array([4095], dtype=">i2")
        rad.attrs["scale_factor"] = np.array([0.04572892], dtype="f4")
        rad.attrs["add_offset"] = np.array([-1.6443], dtype="f4")
        projection = dataset.create_dataset("goes_imager_projection", data=0)
        projection.attrs["perspective_point_height"] = np.array([35786023.0])
        projection.attrs["longitude_of_projection_origin"] = np.array([-75.0])
        projection.attrs["semi_major_axis"] = np.array([6378137.0])
        projection.attrs["semi_minor_axis"] = np.array([6356752.31414])
        projection.attrs["sweep_angle_axis"] = "x"
        for name, scale, offset in (
            ("x", 1.12e-04, -0.151844),
            ("y", -1.12e-04, 0.151844),
        ):
            axis = dataset.create_dataset(name, data=np.arange(size, dtype="i2"))
            axis.attrs["scale_factor"] = np.array([scale], dtype="f4")
            axis.attrs["add_offset"] = np.array([offset], dtype="f4")
        dataset["kappa0"] = 0.0015
        dataset["planck_fk1"] = 10803.3
        dataset["planck_fk2"] = 1392.74
        dataset["planck_bc1"] = 0.07550
        dataset["planck_bc2"] = 0.99975


class _RangeHandler(http.server.SimpleHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    connections: set = set()

    def do_GET(self):
        spec = self.headers.get("Range")
        if spec is None:
            super().do_GET()
            return
        self.connections.add(self.client_address)
        start, end = map(int, spec.removeprefix("bytes=").split("-"))
        with Path(self.translate_path(self.path)).open("rb") as handle:
            handle.seek(start)
            body = handle.read(end - start + 1)
        self.send_response(206)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        """Silence request logging."""


@pytest.fixture
def abi_server(tmp_path):
    """Serve a synthetic ABI file over HTTP with Range support."""
    served = tmp_path / "served"
    served.mkdir()
    server = http.server.ThreadingHTTPServer(
        ("127.0.0.1", 0),
        partial(_RangeHandler, directory=str(served)),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield served, f"http://127.0.0.1:{server.server_port}/"
    server.shutdown()


VILA_VELHA = (-41.2, -21.3, -39.5, -19.6)
MANAUS = (-60.9, -3.9, -59.2, -2.2)


@pytest.mark.parametrize("band", ["C02", "C13"])
def test_crop_reads_a_fraction_and_keeps_renderer_contract(tmp_path, abi_server, band):
    """Test crop reads a fraction and keeps renderer contract."""
    served, url = abi_server
    _write_fake_abi(served / "frame.nc", band)
    output = tmp_path / f"OR_ABI-L1b-RadF-M6{band}_G19_s.tif"

    stats = write_abi_crop(
        f"{url}frame.nc", band, output, VILA_VELHA, user_agent="test"
    )

    assert stats.bytes_read < (served / "frame.nc").stat().st_size / 3
    assert stats.requests > 0 and stats.rows > 0 and stats.tiff_bytes > 0
    assert stats.total_seconds >= stats.network_seconds > 0
    with rasterio.open(output) as dataset:
        assert dataset.count == 2
        assert dataset.dtypes == ("uint8", "uint8")
        assert dataset.crs.to_epsg() == 4326
        window = dataset.window(*VILA_VELHA)
        assert (
            dataset.read(2, window=window.round_offsets().round_lengths()).min() == 255
        )
        assert dataset.read(2)[0, 0] == 0


def test_crop_uses_parallel_connections_and_matches_serial_read(
    tmp_path, abi_server, monkeypatch
):
    """Test crop uses parallel connections and matches serial read."""
    served, url = abi_server
    _write_fake_abi(served / "frame.nc", "C13")
    _RangeHandler.connections.clear()
    parallel = tmp_path / "parallel.tif"
    serial = tmp_path / "serial.tif"

    write_abi_crop(f"{url}frame.nc", "C13", parallel, VILA_VELHA, user_agent="test")
    assert len(_RangeHandler.connections) > 2

    monkeypatch.setattr(partial_read, "READ_AHEAD_BLOCKS", 1)
    write_abi_crop(f"{url}frame.nc", "C13", serial, VILA_VELHA, user_agent="test")

    with rasterio.open(parallel) as first, rasterio.open(serial) as second:
        assert np.array_equal(first.read(), second.read())


def test_read_ahead_stays_off_for_scattered_reads_and_ramps_on_sequential(abi_server):
    """Test read ahead stays off for scattered reads and ramps on sequential."""
    served, url = abi_server
    (served / "blob").write_bytes(bytes(1 << 23))
    remote = partial_read.RangeFile(f"{url}blob", block_size=1 << 16, user_agent="test")

    for index in (40, 3, 20):
        remote._block(index)
    assert sorted(remote._blocks) == [3, 20, 40]

    index = 50
    for _ in range(4):
        remote._block(index)
        while index in remote._blocks:
            index += 1
    remote.close()

    assert index == 50 + 1 + 2 + 4 + 8
    assert index - 50 + 3 == len(remote._blocks) and max(remote._blocks) == 64


def _imagery_downloader(tmp_path: Path, url: str) -> GoesDownloader:
    return GoesDownloader(base_url=url, raw_dir=tmp_path / "raw", band="C13")


C13_NAME = (
    "OR_ABI-L1b-RadF-M6C13_G19_s20260011200209_e20260011209529_c20260011209576.nc"
)


SLOT = "20260011200"


def _crop(area_id: str, bounds, slots=(SLOT,)) -> AreaCrop:
    return AreaCrop(area_id=area_id, bounds=bounds, target_timestamps=slots)


async def _refresh(downloader, monkeypatch, *areas):
    async def listing(_session, _missing=()):
        return [C13_NAME]

    monkeypatch.setattr(downloader, "_fetch_listing", listing)
    return await downloader.refresh_latest(target_timestamps=(SLOT,), areas=areas)


def _serve_c13(served: Path) -> None:
    target = served / "ABI-L1b-RadF/2026/001/12"
    target.mkdir(parents=True)
    _write_fake_abi(target / C13_NAME, "C13")


@pytest.mark.asyncio
async def test_each_area_gets_its_own_folder_and_new_area_leaves_others_alone(
    tmp_path, abi_server, monkeypatch
):
    """Test each area gets its own folder and new area leaves others alone."""
    served, url = abi_server
    _serve_c13(served)
    downloader = _imagery_downloader(tmp_path, url)
    vila_velha = _crop("municipio-3205200", VILA_VELHA)
    manaus = _crop("municipio-1302603", MANAUS)

    first = await _refresh(downloader, monkeypatch, vila_velha)
    assert (first.downloaded_count, first.failed_count) == (1, 0)
    assert first.bytes_read > 0
    original = tmp_path / "raw" / vila_velha.area_id / C13_NAME.replace(".nc", ".tif")
    before = original.stat()

    unchanged = await _refresh(downloader, monkeypatch, vila_velha)
    assert (unchanged.downloaded_count, unchanged.bytes_read) == (0, 0)

    added = await _refresh(downloader, monkeypatch, vila_velha, manaus)
    assert (added.attempted_count, added.downloaded_count) == (1, 1)

    after = original.stat()
    assert (after.st_ino, after.st_mtime_ns) == (before.st_ino, before.st_mtime_ns)
    assert (tmp_path / "raw" / manaus.area_id / original.name).is_file()
    assert len(added.kept_files) == 2


@pytest.mark.asyncio
async def test_area_only_reads_slots_of_its_own_timeline(
    tmp_path, abi_server, monkeypatch
):
    """Test area only reads slots of its own timeline."""
    served, url = abi_server
    _serve_c13(served)
    downloader = _imagery_downloader(tmp_path, url)

    report = await _refresh(
        downloader,
        monkeypatch,
        _crop("wants-it", VILA_VELHA),
        _crop("night-elsewhere", MANAUS, slots=()),
    )

    assert report.downloaded_count == 1
    assert not (tmp_path / "raw" / "night-elsewhere").exists()


@pytest.mark.asyncio
async def test_failed_read_keeps_cycle_alive_and_slot_retryable(
    tmp_path, abi_server, monkeypatch
):
    """Test failed read keeps cycle alive and slot retryable."""
    served, url = abi_server
    monkeypatch.setattr(partial_read.time, "sleep", lambda _seconds: None)
    downloader = _imagery_downloader(tmp_path, url)
    area = _crop("area", VILA_VELHA)

    failed = await _refresh(downloader, monkeypatch, area)
    assert (failed.downloaded_count, failed.failed_count) == (0, 1)
    assert failed.kept_files == []

    _serve_c13(served)
    recovered = await _refresh(downloader, monkeypatch, area)
    assert (recovered.downloaded_count, recovered.failed_count) == (1, 0)


def test_removing_an_area_deletes_only_its_folder(settings):
    """Test removing an area deletes only its folder."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    service = GoesTimelapseService(
        settings=settings,
        catalog=catalog,
        state_store=StateStore(settings.db_path),
        start_background_tasks=False,
    )
    for area_id in ("gone", "stays"):
        folder = settings.raw_dir / "visible" / area_id
        folder.mkdir(parents=True)
        (folder / "OR_ABI-L1b-RadF-M6C02_G19_s2026070180000_e1_c1.tif").write_bytes(
            b"x"
        )

    service._cleanup_area_files("gone")

    assert not (settings.raw_dir / "visible" / "gone").exists()
    assert list(service._available_raw_paths_by_source("gone")["visible"]) == []
    assert list(service._available_raw_paths_by_source("stays")["visible"]) == [
        "20260701800"
    ]


def test_listing_covers_backfill_hours_beyond_lookback(monkeypatch):
    """Test listing covers backfill hours beyond lookback."""

    monkeypatch.setattr(
        downloader_module, "_utc_now", lambda: datetime(2026, 1, 2, 12, tzinfo=UTC)
    )

    hours = downloader_module._listing_hours(("20260010900", "20260010910"))

    assert len(hours) == downloader_module.LOOKBACK_HOURS + 1
    assert datetime(2026, 1, 1, 9, tzinfo=UTC) in hours


@pytest.mark.asyncio
async def test_glm_failed_read_leaves_file_retryable(tmp_path, monkeypatch):
    """Test glm failed read leaves file retryable."""
    downloader = _glm_downloader(tmp_path, monkeypatch)
    payload = bytearray(_glm_payload(tmp_path, 3))
    payload[LAT_NODE] = 0

    async def listing(_session, _missing=()):
        return [GLM_NAME]

    monkeypatch.setattr(downloader, "_fetch_listing", listing)

    class _Session(_FakeSession):
        def __init__(self, *_args, **_kwargs):
            super().__init__(bytes(payload))

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return None

    monkeypatch.setattr(downloader_module.aiohttp, "ClientSession", _Session)
    failed = await downloader.refresh_latest(target_timestamps=("20260011200",))
    assert (failed.downloaded_count, failed.failed_count) == (0, 1)

    payload[LAT_NODE] = ord("T")
    recovered = await downloader.refresh_latest(target_timestamps=("20260011200",))
    assert (recovered.downloaded_count, recovered.failed_count) == (1, 0)
    assert recovered.bytes_read > 0

    record = json.loads((tmp_path / "raw" / "20260011200_glm.json").read_text())
    assert record["source_filenames"] == [GLM_NAME]
    assert len(record["flashes"]) == 3


@pytest.mark.asyncio
async def test_glm_swapped_nodes_stop_reads_until_layout_matches(
    tmp_path, abi_server, monkeypatch
):
    """Test glm swapped nodes stop reads until layout matches."""
    served, url = abi_server
    source = served / "GLM-L2-LCFA/2026/001/12" / GLM_NAME
    source.parent.mkdir(parents=True)
    source.write_bytes(_glm_payload(tmp_path, 50))
    monkeypatch.setattr(downloader_module, "GLM_LAT_NODE", LON_NODE)
    monkeypatch.setattr(downloader_module, "GLM_LON_NODE", LAT_NODE)
    downloader = GlmDownloader(base_url=url, raw_dir=tmp_path / "raw")

    async def listing(_session, _missing=()):
        return [GLM_NAME]

    monkeypatch.setattr(downloader, "_fetch_listing", listing)
    stopped = await downloader.refresh_latest(target_timestamps=("20260011200",))
    assert (stopped.downloaded_count, stopped.failed_files) == (0, [GLM_NAME])
    assert not list((tmp_path / "raw").glob("*_glm.json"))

    monkeypatch.setattr(downloader_module, "GLM_LAT_NODE", LAT_NODE)
    monkeypatch.setattr(downloader_module, "GLM_LON_NODE", LON_NODE)
    recovered = await downloader.refresh_latest(target_timestamps=("20260011200",))
    assert (recovered.downloaded_count, recovered.failed_count) == (1, 0)

    monkeypatch.setattr(downloader_module, "read_glm_flashes_with_hdf5", None)
    assert await downloader._layout_verified(None, GLM_NAME)


@pytest.mark.asyncio
async def test_glm_inconclusive_check_withholds_flashes(
    tmp_path, abi_server, monkeypatch
):
    """Test glm inconclusive check withholds flashes."""
    served, url = abi_server
    source = served / "GLM-L2-LCFA/2026/001/12" / GLM_NAME
    source.parent.mkdir(parents=True)
    source.write_bytes(_glm_payload(tmp_path, 50))
    monkeypatch.setattr(downloader_module, "GLM_LAT_NODE", LAT_NODE)
    monkeypatch.setattr(downloader_module, "GLM_LON_NODE", LON_NODE)
    downloader = GlmDownloader(base_url=url, raw_dir=tmp_path / "raw")

    async def listing(_session, _missing=()):
        return [GLM_NAME]

    def renamed_column(_url, **_kwargs):
        raise KeyError("flash_lat")

    monkeypatch.setattr(downloader, "_fetch_listing", listing)
    reference = downloader_module.read_glm_flashes_with_hdf5
    monkeypatch.setattr(downloader_module, "read_glm_flashes_with_hdf5", renamed_column)
    withheld = await downloader.refresh_latest(target_timestamps=("20260011200",))
    assert (withheld.downloaded_count, withheld.failed_files) == (0, [GLM_NAME])
    assert not list((tmp_path / "raw").glob("*_glm.json"))

    monkeypatch.setattr(downloader_module, "read_glm_flashes_with_hdf5", reference)
    recovered = await downloader.refresh_latest(target_timestamps=("20260011200",))
    assert (recovered.downloaded_count, recovered.failed_count) == (1, 0)


@pytest.mark.asyncio
async def test_failed_source_with_cached_files_reaches_the_service_summary(
    settings, monkeypatch
):
    """Test failed source with cached files reaches the service summary."""
    catalog = AreaCatalog.from_path(settings.catalog_path)
    service = GoesTimelapseService(
        settings=settings,
        catalog=catalog,
        state_store=StateStore(settings.db_path),
        start_background_tasks=False,
    )

    async def plans():
        return [
            SimpleNamespace(
                source_key="visible",
                should_download=True,
                reason="test",
                target_timestamps=(),
            )
        ]

    async def noaa_down(_plan):
        raise OSError("NOAA fora do ar")

    monkeypatch.setattr(service, "_build_download_plans", plans)
    monkeypatch.setattr(service, "_apply_download_plan", lambda _plan: None)
    monkeypatch.setattr(service, "_refresh_source", noaa_down)
    monkeypatch.setattr(service, "_raw_files_for_source", lambda _key: ["cached.tif"])
    monkeypatch.setattr(service, "_mark_source_partial_due_to_error", lambda _key: None)
    await service.refresh_raw_frames()
    assert "NOAA fora do ar" in str(service._status["last_poll_error"])
