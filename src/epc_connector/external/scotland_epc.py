"""
Download Scottish EPC open data into one Parquet file per series.

Each series is the publisher's "Extended Historic" 7z archive on data.gov.scot:
every certificate ever lodged, one CSV per year, with superseded certificates
flagged by ``HISTORIC``. The archive is streamed to a staging directory, its
CSVs are extracted and streamed in time order into ``data/<series>.parquet``,
and the staging files are deleted. Values stay as published strings; the label
row (row 2) of each CSV is skipped.
"""

import contextlib
import logging
import re
import shutil
import tempfile
from enum import StrEnum
from pathlib import Path
from typing import Final

import httpx

from epc_connector.ingest import (
    DEFAULT_CHUNK_ROWS,
    DEFAULT_ROW_GROUP_ROWS,
    download_file,
    extract_files,
    merge_csvs_to_parquet,
)

logger = logging.getLogger(__name__)


class ScotlandEpcDataset(StrEnum):
    """
    Scottish EPC series published on data.gov.scot.

    Both are the publisher's "Extended Historic" datasets: every certificate
    ever lodged, with ``HISTORIC = True`` marking certificates superseded by a
    newer one for the same property.

    Attributes
    ----------
    DOMESTIC
        Every domestic EPC lodged since December 2008.
    NON_DOMESTIC
        Every non-domestic EPC lodged since 2013.
    """

    DOMESTIC = "domestic"
    NON_DOMESTIC = "non-domestic"


DOMESTIC_URL: Final = (
    "https://api.data.gov.scot/resource/b7bc304c-debb-4365-9a86-1c8cb82cc60d/download"
)
NON_DOMESTIC_URL: Final = (
    "https://api.data.gov.scot/resource/c7873ae4-d19a-43ee-8d9f-415e7d0f05d7/download"
)
SOURCE_URLS: Final[dict[ScotlandEpcDataset, str]] = {
    ScotlandEpcDataset.DOMESTIC: DOMESTIC_URL,
    ScotlandEpcDataset.NON_DOMESTIC: NON_DOMESTIC_URL,
}
PARQUET_NAMES: Final[dict[ScotlandEpcDataset, str]] = {
    ScotlandEpcDataset.DOMESTIC: "domestic_epc_certificate_scotland.parquet",
    ScotlandEpcDataset.NON_DOMESTIC: "non_domestic_epc_certificate_scotland.parquet",
}

_PERIOD: Final = re.compile(r"^(?:Q([1-4]) )?(\d{4})$")


def download_scotland_epc(
    dataset: ScotlandEpcDataset | str,
    *,
    data_dir: Path | str = "data",
    staging_dir: Path | str = ".staging",
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
    row_group_rows: int = DEFAULT_ROW_GROUP_ROWS,
    client: httpx.Client | None = None,
) -> Path:
    """
    Download a Scottish EPC series and merge it into one Parquet file.

    An existing file for the series is replaced once the new one is complete.

    Parameters
    ----------
    dataset
        Series to download, e.g. ``"domestic"``.
    data_dir
        Directory for the Parquet file.
    staging_dir
        Directory for the archive and extracted CSVs; emptied afterwards.
    chunk_rows, row_group_rows
        Memory limits for the merge; see ``merge_csvs_to_parquet``.
    client
        HTTP client to use; a temporary one is created if omitted.

    Returns
    -------
    Path
        Path of the Parquet file, e.g.
        ``data/domestic_epc_certificate_scotland.parquet``.
    """
    dataset = ScotlandEpcDataset(dataset)
    data_dir, staging_dir = Path(data_dir), Path(staging_dir)
    parquet_path = data_dir / PARQUET_NAMES[dataset]

    staging_dir.mkdir(parents=True, exist_ok=True)
    run_dir = Path(tempfile.mkdtemp(prefix=f"{dataset}-", dir=staging_dir))
    try:
        archive_path = run_dir / "archive.7z"
        logger.info("Downloading %s", dataset)
        download_file(SOURCE_URLS[dataset], archive_path, client=client)

        csv_paths = extract_files(archive_path, run_dir / "csv", suffix=".csv")
        archive_path.unlink()  # free disk before the merge
        merged_path = run_dir / "merged.parquet"
        row_count = merge_csvs_to_parquet(
            sorted(csv_paths, key=period_sort_key),
            merged_path,
            skip_rows_after_header=1,  # descriptive label row
            chunk_rows=chunk_rows,
            row_group_rows=row_group_rows,
        )

        data_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(merged_path, parquet_path)
        logger.info("Wrote %s (%d rows)", parquet_path, row_count)
        return parquet_path
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)
        with contextlib.suppress(OSError):
            staging_dir.rmdir()  # only succeeds if no other run is using it


def period_sort_key(path: Path) -> tuple[int, int, str]:
    """Sort key ordering ``Q3 2016.csv`` and ``2017.csv`` files oldest first."""
    match = _PERIOD.match(path.stem)
    if match is None:
        return 0, 0, path.name
    quarter, year = match.groups()
    return int(year), int(quarter or 0), path.name
