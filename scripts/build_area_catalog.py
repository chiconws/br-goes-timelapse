from __future__ import annotations

import argparse
import gzip
import json
from pathlib import Path
import re
from typing import cast
import unicodedata
from urllib.parse import urlparse
import urllib.request

import pandas as pd

DEFAULT_POPULATION_SOURCE = "https://ftp.ibge.gov.br/Estimativas_de_Populacao/Estimativas_2025/POP2025_20260113.ods"
DEFAULT_LOCALIDADES_BASE_URL = "https://servicodados.ibge.gov.br/api/v1/localidades"
SUPPORTED_AREA_TYPES = frozenset({"municipio"})


def normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    normalized = re.sub(r"[^0-9a-z]+", " ", normalized.lower())
    return " ".join(normalized.split())


def build_area_catalog(
    output_path: Path,
    *,
    population_source: str | Path | None,
    localidades_base_url: str = DEFAULT_LOCALIDADES_BASE_URL,
) -> list[dict[str, object]]:
    population_by_ibge = load_population_estimates(population_source)
    payloads = _load_localidades_payloads(localidades_base_url)
    rows = _build_area_rows(payloads, population_by_ibge=population_by_ibge)
    _write_rows(output_path, rows)
    return rows


def load_population_estimates(source: str | Path | None) -> dict[str, int]:
    if not source:
        return {}

    source_value = str(source)
    extension = Path(urlparse(source_value).path).suffix.lower()
    engine = "odf" if extension == ".ods" else None
    workbook = pd.ExcelFile(source_value, engine=engine)
    sheet_name = next(
        (
            name
            for name in workbook.sheet_names
            if normalize_text(name).startswith("municipios")
        ),
        None,
    )
    if sheet_name is None:
        raise ValueError(f"No municipality sheet found in {source_value}")

    frame = cast(pd.DataFrame, workbook.parse(sheet_name=sheet_name, header=1))
    municipality_columns = ["COD. UF", "COD. MUNIC", "POPULAÇÃO ESTIMADA"]
    code_rows = cast(
        pd.DataFrame,
        frame.loc[:, municipality_columns],
    ).dropna()

    population_by_ibge: dict[str, int] = {}
    for _, record in code_rows.iterrows():
        state_code = int(record["COD. UF"])
        municipality_code = int(record["COD. MUNIC"])
        ibge_code = f"{state_code:02d}{municipality_code:05d}"
        population_by_ibge[ibge_code] = int(record["POPULAÇÃO ESTIMADA"])
    return population_by_ibge


def _load_localidades_payloads(base_url: str) -> dict[str, list[dict[str, object]]]:
    base = base_url.rstrip("/")
    return {
        "estados": _load_json_url(f"{base}/estados"),
        "municipios": _load_json_url(f"{base}/municipios"),
    }


def _build_area_rows(
    payloads: dict[str, list[dict[str, object]]],
    *,
    population_by_ibge: dict[str, int],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for municipality in payloads["municipios"]:
        immediate_region = cast(dict[str, object], municipality["regiao-imediata"])
        intermediate_region = cast(
            dict[str, object], immediate_region["regiao-intermediaria"]
        )
        state = cast(dict[str, object], intermediate_region["UF"])
        ibge_code = str(municipality["id"])
        rows.append(
            _area_row(
                area_type="municipio",
                area_code=ibge_code,
                name=str(municipality["nome"]),
                state_code=str(state["sigla"]),
                state_name=str(state["nome"]),
                parent_code=None,
                parent_name=None,
                population=population_by_ibge.get(ibge_code),
                extra_search=[
                    ibge_code,
                    str(state["sigla"]),
                    str(state["nome"]),
                    "municipio",
                ],
            )
        )

    rows.sort(key=lambda item: str(item["area_id"]))
    return [row for row in rows if row["area_type"] in SUPPORTED_AREA_TYPES]


def _area_row(
    *,
    area_type: str,
    area_code: str,
    name: str,
    state_code: str | None,
    state_name: str | None,
    parent_code: str | None,
    parent_name: str | None,
    population: int | None,
    extra_search: list[str],
) -> dict[str, object]:
    parts = [area_code, name, *extra_search]
    if state_code:
        parts.append(state_code)
    if state_name:
        parts.append(state_name)
    if parent_name:
        parts.append(parent_name)
    return {
        "area_id": f"{area_type}-{area_code}",
        "area_type": area_type,
        "area_code": area_code,
        "name": name,
        "search_text": normalize_text(" ".join(parts)),
        "population": population,
        "state_code": state_code,
        "state_name": state_name,
        "parent_code": parent_code,
        "parent_name": parent_name,
    }


def _load_json_url(url: str) -> list[dict[str, object]]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "User-Agent": "goes-timelapse-addon/0.1",
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        raw = response.read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"Unexpected payload from {url}")
    return payload


def _write_rows(output_path: Path, rows: list[dict[str, object]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(output_path, "wt", encoding="utf-8") as handle:
        json.dump(rows, handle, ensure_ascii=False, separators=(",", ":"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Build GOES Timelapse area catalog.")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("goes_timelapse/src/goes_timelapse/assets/areas.json.gz"),
        help="Output path for the compressed runtime catalog.",
    )
    parser.add_argument(
        "--population-source",
        default=DEFAULT_POPULATION_SOURCE,
        help="Path or URL to the latest official IBGE municipality population spreadsheet.",
    )
    parser.add_argument(
        "--localidades-base-url",
        default=DEFAULT_LOCALIDADES_BASE_URL,
        help="Base URL for the IBGE Localidades API.",
    )
    args = parser.parse_args()
    rows = build_area_catalog(
        args.output,
        population_source=args.population_source,
        localidades_base_url=args.localidades_base_url,
    )
    print(f"Wrote {len(rows)} areas to {args.output}")


if __name__ == "__main__":
    main()
