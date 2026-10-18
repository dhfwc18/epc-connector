"""
Download and update Scottish EPC open data as one Parquet file per series.

Each series is the publisher's "Extended Historic" 7z archive on data.gov.scot:
every certificate ever lodged, one CSV per year of ``CREATED_AT`` (lodgement
time), with superseded certificates flagged by ``HISTORIC``. Values stay as
published strings; the label row (row 2) of each CSV is skipped, and the data
is checked against ``schemas/source/scotland/<series>.schema.json``. The
release (e.g. ``2026q2``) is stored in the Parquet file's metadata so updates
can tell whether a newer release exists.
"""

import contextlib
import datetime as dt
import functools
import logging
import re
from collections.abc import Callable
from enum import StrEnum
from pathlib import Path
from typing import Any, Final

import httpx
import polars as pl

from epc_connector.datasets import RELEASE_METADATA_KEY, Origin, get_dataset
from epc_connector.storage import (
    DEFAULT_CHUNK_ROWS,
    DEFAULT_ROW_GROUP_ROWS,
    HTTP_TIMEOUT,
    StorageError,
    archive_url_to_parquet,
    atomic_write,
    staging_directory,
    stream_to_parquet,
)
from epc_connector.validation import DEFAULT_SCHEMAS_DIR, load_schema

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


CKAN_API_URL: Final = "https://api.data.gov.scot/api/3/action"
"""Base URL of the data.gov.scot CKAN API."""
DOMESTIC_URL: Final = (
    "https://api.data.gov.scot/resource/b7bc304c-debb-4365-9a86-1c8cb82cc60d/download"
)
"""Download URL of the domestic Extended Historic archive."""
NON_DOMESTIC_URL: Final = (
    "https://api.data.gov.scot/resource/c7873ae4-d19a-43ee-8d9f-415e7d0f05d7/download"
)
"""Download URL of the non-domestic Extended Historic archive."""
SOURCE_URLS: Final[dict[ScotlandEpcDataset, str]] = {
    ScotlandEpcDataset.DOMESTIC: DOMESTIC_URL,
    ScotlandEpcDataset.NON_DOMESTIC: NON_DOMESTIC_URL,
}
"""Archive download URL, by series."""
PARQUET_NAMES: Final[dict[ScotlandEpcDataset, str]] = {
    series: get_dataset(Origin.SCOTLAND, series).parquet_name
    for series in ScotlandEpcDataset
}
"""Local file name, by series (from ``datasets.DATASETS``)."""


_CREATED_DATE: Final = (
    pl.col("CREATED_AT").str.to_datetime("%d/%m/%Y %H:%M:%S", strict=False).dt.date()
)
_RESOURCE_ID: Final = re.compile(r"/resource/([0-9a-f-]{36})/download")
_RELEASE_IN_TITLE: Final = re.compile(r"\bQ([1-4])\s+(\d{4})\b")
_YEAR_FILE: Final = re.compile(r"^(\d{4})$")

type DateWindow = tuple[dt.date, dt.date | None]


def download_scotland_epc(
    dataset: ScotlandEpcDataset | str,
    *,
    data_dir: Path | str = "data",
    staging_dir: Path | str = ".staging",
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
    row_group_rows: int = DEFAULT_ROW_GROUP_ROWS,
    schemas_dir: Path | str = DEFAULT_SCHEMAS_DIR,
    client: httpx.Client | None = None,
) -> Path:
    """
    Download a Scottish EPC series and merge it into one Parquet file.

    Column names are checked against the schema before writing and every value
    before the file is moved into ``data_dir``. An existing file is replaced
    only once the new one is complete and valid.

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
    schemas_dir
        Directory holding ``source/scotland/<series>.schema.json``.
    client
        HTTP client to use; a temporary one is created if omitted.

    Returns
    -------
    Path
        Path of the Parquet file, e.g.
        ``data/domestic_epc_certificate_scotland.parquet``.
    """
    return _build_series(
        ScotlandEpcDataset(dataset),
        data_dir=Path(data_dir),
        staging_dir=Path(staging_dir),
        chunk_rows=chunk_rows,
        row_group_rows=row_group_rows,
        schemas_dir=Path(schemas_dir),
        client=client,
    )


def update_scotland_epc(
    dataset: ScotlandEpcDataset | str,
    *,
    date_from: dt.date | str | None = None,
    date_to: dt.date | str | None = None,
    data_dir: Path | str = "data",
    staging_dir: Path | str = ".staging",
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
    row_group_rows: int = DEFAULT_ROW_GROUP_ROWS,
    schemas_dir: Path | str = DEFAULT_SCHEMAS_DIR,
    client: httpx.Client | None = None,
) -> Path:
    """
    Refresh a local Parquet file from the latest release.

    Works like ``download_scotland_epc``, except that only the yearly CSVs
    overlapping ``[date_from, date_to]`` (by ``CREATED_AT`` date) are
    extracted, and only rows in that window are replaced; rows outside it are
    kept. New certificates are therefore added and re-published ones
    refreshed, without duplicates. ``HISTORIC`` flags outside the window are
    not refreshed.

    Parameters
    ----------
    dataset
        Series to update, e.g. ``"domestic"``.
    date_from
        First ``CREATED_AT`` date to refresh (date or ``"YYYY-MM-DD"``).
        Defaults to the latest date in the local file, ignoring future dates
        (entry errors). Without ``date_from`` and ``date_to`` nothing is
        downloaded if the local file already holds the latest release.
    date_to
        Last ``CREATED_AT`` date to refresh; open-ended by default.
    data_dir, staging_dir, chunk_rows, row_group_rows, schemas_dir, client
        As for ``download_scotland_epc``.

    Returns
    -------
    Path
        Path of the updated Parquet file.

    Raises
    ------
    FileNotFoundError
        If there is no local file yet; run ``download_scotland_epc`` first.
    ValueError
        If ``date_to`` is before ``date_from``.
    """
    dataset = ScotlandEpcDataset(dataset)
    parquet_path = Path(data_dir) / PARQUET_NAMES[dataset]
    if not parquet_path.is_file():
        raise FileNotFoundError(
            f"{parquet_path} not found; run download_scotland_epc first"
        )
    start = _as_date(date_from) or latest_created_date(parquet_path) or dt.date.min
    end = _as_date(date_to)
    if end is not None and end < start:
        raise ValueError(f"date_to {end} is before date_from {start}")

    explicit_window = date_from is not None or date_to is not None
    return _build_series(
        dataset,
        data_dir=Path(data_dir),
        staging_dir=Path(staging_dir),
        chunk_rows=chunk_rows,
        row_group_rows=row_group_rows,
        schemas_dir=Path(schemas_dir),
        client=client,
        window=(start, end),
        current_release=None if explicit_window else stored_release(parquet_path),
    )


def fetch_release(dataset: ScotlandEpcDataset, client: httpx.Client) -> str:
    """Return the dataset's current release, e.g. ``"2026q2"``, from CKAN."""
    resource_id = _RESOURCE_ID.search(SOURCE_URLS[dataset])
    if resource_id is None:
        raise StorageError(f"no resource id in {SOURCE_URLS[dataset]}")
    resource = _ckan_get(client, "resource_show", resource_id[1])
    package = _ckan_get(client, "package_show", resource["package_id"])
    return release_from_title(package["title"])


def release_from_title(title: str) -> str:
    """Return ``"2026q2"`` for a title ending ``"... Dataset to Q2 2026"``."""
    match = _RELEASE_IN_TITLE.search(title)
    if match is None:
        raise StorageError(f"no release quarter in title {title!r}")
    quarter, year = match.groups()
    return f"{year}q{quarter}"


def stored_release(parquet_path: Path) -> str | None:
    """Return the release recorded in a Parquet file, if any."""
    return pl.read_parquet_metadata(parquet_path).get(RELEASE_METADATA_KEY)


def latest_created_date(
    parquet_path: Path, *, today: dt.date | None = None
) -> dt.date | None:
    """Return the latest ``CREATED_AT`` date that is not after ``today``."""
    today = today or dt.date.today()
    return (
        pl.scan_parquet(parquet_path, low_memory=True)
        .select(_CREATED_DATE.filter(_CREATED_DATE.le(today)).max())
        .collect(engine="streaming")
        .item()
    )


def period_sort_key(path: Path) -> tuple[int, str]:
    """Sort key ordering ``2008.csv``, ``2009.csv`` ... oldest first."""
    match = _YEAR_FILE.match(path.stem)
    return (int(match[1]) if match else 0), path.name


def _build_series(
    dataset: ScotlandEpcDataset,
    *,
    data_dir: Path,
    staging_dir: Path,
    chunk_rows: int,
    row_group_rows: int,
    schemas_dir: Path,
    client: httpx.Client | None,
    window: DateWindow | None = None,
    current_release: str | None = None,
) -> Path:
    """
    Download, validate and merge a series into ``data_dir``.

    With ``window``, only rows in that ``CREATED_AT`` window are taken from the
    download and the local file's other rows are kept. Nothing is downloaded if
    the latest release equals ``current_release``.
    """
    parquet_path = data_dir / PARQUET_NAMES[dataset]
    schema = load_schema(schemas_dir / "source" / "scotland" / f"{dataset}.schema.json")
    with (
        (
            contextlib.nullcontext(client)
            if client is not None
            else httpx.Client(timeout=HTTP_TIMEOUT)
        ) as http,
        staging_directory(staging_dir, prefix=f"{dataset}-") as run_dir,
    ):
        release = fetch_release(dataset, http)
        if release == current_release:
            logger.info("%s is already at release %s", parquet_path, release)
            return parquet_path

        logger.info("Downloading %s (release %s)", dataset, release)
        metadata = {RELEASE_METADATA_KEY: release}
        download_to = functools.partial(
            archive_url_to_parquet,
            SOURCE_URLS[dataset],
            work_dir=run_dir,
            client=http,
            include=None if window is None else _year_filter(window),
            sort_key=period_sort_key,
            skip_rows_after_header=1,  # descriptive label row
            chunk_rows=chunk_rows,
            row_group_rows=row_group_rows,
            schema=schema,
            metadata=metadata,
        )
        with atomic_write(parquet_path, run_dir) as output_path:
            if window is None:
                download_to(output_path)
            else:
                window_path = run_dir / "window.parquet"
                download_to(window_path)
                in_window = _created_between(*window)
                stream_to_parquet(
                    pl.concat(
                        [
                            pl.scan_parquet(parquet_path).filter(~in_window),
                            pl.scan_parquet(window_path).filter(in_window),
                        ]
                    ),
                    output_path,
                    chunk_rows=chunk_rows,
                    row_group_rows=row_group_rows,
                    metadata=metadata,
                )
        logger.info("Wrote %s (release %s)", parquet_path, release)
        return parquet_path


def _ckan_get(client: httpx.Client, action: str, item_id: str) -> dict[str, Any]:
    response = client.get(f"{CKAN_API_URL}/{action}", params={"id": item_id})
    response.raise_for_status()
    return response.json()["result"]


def _as_date(value: dt.date | str | None) -> dt.date | None:
    if isinstance(value, str):
        return dt.date.fromisoformat(value)
    return value


def _created_between(start: dt.date, end: dt.date | None) -> pl.Expr:
    """True for rows whose ``CREATED_AT`` date is in ``[start, end]``."""
    if end is None:
        return _CREATED_DATE.ge(start).fill_null(False)
    return _CREATED_DATE.is_between(start, end).fill_null(False)


def _year_filter(window: DateWindow) -> Callable[[str], bool]:
    """Return a member-name filter for yearly CSVs overlapping ``window``."""
    start, end = window

    def overlaps(name: str) -> bool:
        match = _YEAR_FILE.match(Path(name).stem)
        if match is None:
            return True  # not a yearly file (e.g. one CSV for all years)
        year = int(match[1])
        return year >= start.year and (end is None or year <= end.year)

    return overlaps
