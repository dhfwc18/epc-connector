"""
Download England and Wales EPC open data into one Parquet file per series.

Each series is a bulk ZIP from Get energy performance of buildings data,
regenerated monthly: one ``certificates-YYYY.csv`` per year of
``lodgement_date``, plus ``recommendations-YYYY.csv`` files for some series,
which are skipped. The API needs a bearer token, taken from ``EPB_API_TOKEN``
unless passed explicitly. Values stay as published strings and are checked
against ``schemas/source/england-and-wales/<series>.schema.json``. The file's
``lastUpdated`` timestamp is stored in the Parquet metadata.
"""

import contextlib
import logging
import os
import re
import shutil
from enum import StrEnum
from pathlib import Path
from typing import Any, Final

import httpx
from dotenv import find_dotenv, load_dotenv

from epc_connector.ingest import (
    DEFAULT_CHUNK_ROWS,
    DEFAULT_ROW_GROUP_ROWS,
    HTTP_TIMEOUT,
    archive_url_to_parquet,
    staging_directory,
)
from epc_connector.validation import load_schema

logger = logging.getLogger(__name__)


class EnglandWalesEpcDataset(StrEnum):
    """
    England and Wales EPC series from Get energy performance of buildings data.

    Attributes
    ----------
    DOMESTIC
        Domestic Energy Performance Certificates.
    NON_DOMESTIC
        Non-domestic Energy Performance Certificates.
    DISPLAY
        Display Energy Certificates (DECs) for public buildings.
    """

    DOMESTIC = "domestic"
    NON_DOMESTIC = "non-domestic"
    DISPLAY = "display"


API_URL: Final = "https://api.get-energy-performance-data.communities.gov.uk"
DOMESTIC_URL: Final = f"{API_URL}/api/files/domestic/csv"
NON_DOMESTIC_URL: Final = f"{API_URL}/api/files/non-domestic/csv"
DISPLAY_URL: Final = f"{API_URL}/api/files/display/csv"
SOURCE_URLS: Final[dict[EnglandWalesEpcDataset, str]] = {
    EnglandWalesEpcDataset.DOMESTIC: DOMESTIC_URL,
    EnglandWalesEpcDataset.NON_DOMESTIC: NON_DOMESTIC_URL,
    EnglandWalesEpcDataset.DISPLAY: DISPLAY_URL,
}
PARQUET_NAMES: Final[dict[EnglandWalesEpcDataset, str]] = {
    EnglandWalesEpcDataset.DOMESTIC: "domestic_epc_certificate_england_wales.parquet",
    EnglandWalesEpcDataset.NON_DOMESTIC: (
        "non_domestic_epc_certificate_england_wales.parquet"
    ),
    EnglandWalesEpcDataset.DISPLAY: "display_energy_certificate_england_wales.parquet",
}

API_TOKEN_ENV_VAR: Final = "EPB_API_TOKEN"
LAST_UPDATED_METADATA_KEY: Final = "epc_connector.last_updated"
# Repository ``schemas/`` directory (src/epc_connector/external -> repo root).
DEFAULT_SCHEMAS_DIR: Final = Path(__file__).resolve().parents[3] / "schemas"

_CERTIFICATES_FILE: Final = re.compile(r"^certificates-(\d{4})\.csv$")


def download_england_and_wales_epc(
    dataset: EnglandWalesEpcDataset | str,
    *,
    api_token: str | None = None,
    data_dir: Path | str = "data",
    staging_dir: Path | str = ".staging",
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
    row_group_rows: int = DEFAULT_ROW_GROUP_ROWS,
    schemas_dir: Path | str = DEFAULT_SCHEMAS_DIR,
    client: httpx.Client | None = None,
) -> Path:
    """
    Download an England and Wales EPC series and merge it into one Parquet file.

    Column names are checked against the schema before writing and every value
    before the file is moved into ``data_dir``. An existing file is replaced
    only once the new one is complete and valid.

    Parameters
    ----------
    dataset
        Series to download, e.g. ``"domestic"``.
    api_token
        API bearer token; defaults to the ``EPB_API_TOKEN`` environment
        variable (or ``.env``). Ignored when ``client`` is given.
    data_dir
        Directory for the Parquet file.
    staging_dir
        Directory for the archive and extracted CSVs; emptied afterwards.
    chunk_rows, row_group_rows
        Memory limits for the merge; see ``merge_csvs_to_parquet``.
    schemas_dir
        Directory holding ``source/england-and-wales/<series>.schema.json``.
    client
        Authorised HTTP client to use, e.g. from ``authorised_client``.

    Returns
    -------
    Path
        Path of the Parquet file, e.g.
        ``data/domestic_epc_certificate_england_wales.parquet``.
    """
    dataset = EnglandWalesEpcDataset(dataset)
    parquet_path = Path(data_dir) / PARQUET_NAMES[dataset]
    schema = load_schema(
        Path(schemas_dir) / "source" / "england-and-wales" / f"{dataset}.schema.json"
    )
    with (
        (
            contextlib.nullcontext(client)
            if client is not None
            else authorised_client(api_token, timeout=HTTP_TIMEOUT)
        ) as http,
        staging_directory(Path(staging_dir), prefix=f"ew-{dataset}-") as run_dir,
    ):
        last_updated = fetch_last_updated(dataset, http)
        logger.info("Downloading %s (published %s)", dataset, last_updated)
        merged_path = run_dir / "merged.parquet"
        row_count = archive_url_to_parquet(
            SOURCE_URLS[dataset],
            merged_path,
            work_dir=run_dir,
            client=http,
            include=is_certificates_file,
            sort_key=year_sort_key,
            chunk_rows=chunk_rows,
            row_group_rows=row_group_rows,
            schema=schema,
            metadata={LAST_UPDATED_METADATA_KEY: last_updated},
        )
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(merged_path, parquet_path)
    logger.info("Wrote %s (%d rows)", parquet_path, row_count)
    return parquet_path


def authorised_client(api_token: str | None = None, **options: Any) -> httpx.Client:
    """
    Return an ``httpx.Client`` that sends the API bearer token.

    httpx drops the token when following the redirect to the pre-signed S3
    download URL, which must not receive it.

    Parameters
    ----------
    api_token
        Bearer token; defaults to the ``EPB_API_TOKEN`` environment variable,
        loaded from the nearest ``.env`` (searching up from the working
        directory) if it is not already set.
    **options
        Passed on to ``httpx.Client``.

    Raises
    ------
    ValueError
        If no token is given, set in the environment or found in ``.env``.
    """
    if api_token is None and API_TOKEN_ENV_VAR not in os.environ:
        load_dotenv(find_dotenv(usecwd=True))
    token = api_token or os.environ.get(API_TOKEN_ENV_VAR)
    if not token:
        raise ValueError(f"no API token: pass api_token or set {API_TOKEN_ENV_VAR}")
    return httpx.Client(headers={"Authorization": f"Bearer {token}"}, **options)


def fetch_last_updated(dataset: EnglandWalesEpcDataset, client: httpx.Client) -> str:
    """Return when the series' bulk file was last regenerated (ISO timestamp)."""
    response = client.get(f"{SOURCE_URLS[dataset]}/info")
    response.raise_for_status()
    return response.json()["data"]["lastUpdated"]


def is_certificates_file(name: str) -> bool:
    """True for ``certificates-YYYY.csv`` members (not recommendations)."""
    return _CERTIFICATES_FILE.match(Path(name).name) is not None


def year_sort_key(path: Path) -> tuple[int, str]:
    """Sort key ordering ``certificates-YYYY.csv`` files oldest first."""
    match = _CERTIFICATES_FILE.match(path.name)
    return (int(match[1]) if match else 0), path.name
