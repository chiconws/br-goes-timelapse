from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path


BRAZIL_LONLAT_BBOX = (-75.5, -35.5, -30.5, 7.5)


class Geo2GridConverter:
    def __init__(
        self,
        *,
        command: str = "geo2grid.sh",
        product: str = "C02",
        ll_bbox: tuple[float, float, float, float] = BRAZIL_LONLAT_BBOX,
        grid: str = "wgs84_fit",
        method: str = "nearest",
        num_workers: int = 1,
        scratch_dir: Path | None = None,
    ) -> None:
        self._command = command
        self._product = product
        self._ll_bbox = ll_bbox
        self._grid = grid
        self._method = method
        self._num_workers = num_workers
        self._scratch_dir = scratch_dir

    def set_ll_bbox(self, ll_bbox: tuple[float, float, float, float]) -> None:
        self._ll_bbox = ll_bbox

    def output_filename(self, source_filename: str) -> str:
        return f"{Path(source_filename).stem}.tif"

    def source_filename(self, output_filename: str) -> str:
        return f"{Path(output_filename).stem}.nc"

    def convert(self, source_path: Path, output_path: Path) -> None:
        if shutil.which(self._command) is None:
            raise RuntimeError(f"'{self._command}' não está disponível no ambiente")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        temp_root = self._scratch_dir or Path(tempfile.gettempdir())
        temp_root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="geo2grid-", dir=temp_root) as temp_dir:
            temporary_output = Path(temp_dir) / "frame.tif"
            command = [
                self._command,
                "-r",
                "abi_l1b",
                "-w",
                "geotiff",
                "-p",
                self._product,
                "-g",
                self._grid,
                "--method",
                self._method,
                "--num-workers",
                str(self._num_workers),
                "--ll-bbox",
                *(str(value) for value in self._ll_bbox),
                "--output-filename",
                str(temporary_output),
                "-f",
                str(source_path),
            ]
            env = {
                **os.environ,
                # Reading netCDF/HDF5 files from NFS mounts is prone to lock issues.
                "HDF5_USE_FILE_LOCKING": "FALSE",
            }
            try:
                subprocess.run(
                    command,
                    check=True,
                    capture_output=True,
                    env=env,
                    text=True,
                    timeout=900,
                )
            except subprocess.CalledProcessError as err:
                stderr = (err.stderr or "").strip()
                stdout = (err.stdout or "").strip()
                detail = stderr or stdout or str(err)
                raise RuntimeError(f"Geo2Grid falhou para {source_path.name}: {detail}") from err

            if not temporary_output.exists():
                raise RuntimeError(
                    f"Geo2Grid não gerou o GeoTIFF esperado para {source_path.name}"
                )

            output_path.unlink(missing_ok=True)
            shutil.move(str(temporary_output), output_path)
