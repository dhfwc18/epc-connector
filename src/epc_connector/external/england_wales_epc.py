"""
Download England and Wales EPC open data into one Parquet file per series.

Each series is a bulk ZIP from Get energy performance of buildings data,
regenerated monthly: one ``certificates-YYYY.csv`` per year of
``lodgement_date``, plus ``recommendations-YYYY.csv`` files for some series,
which are skipped. The API needs a bearer token, taken from ``EPB_API_TOKEN``
unless passed explicitly. Values stay as published strings and are checked
against ``schemas/source/england-and-wales/<series>.schema.json``. The file's
``lastUpdated`` timestamp is stored in the Parquet metadata.

``update_england_and_wales_epc`` refreshes a local file from the search and
deltas APIs, reading only the window's yearly CSVs from the bulk ZIP and
patching changed UPRNs from the certificate API.
"""

import contextlib
import datetime as dt
import logging
import os
import re
import shutil
import time
from enum import StrEnum
from pathlib import Path
from typing import Any, Final

import httpx
import polars as pl
from dotenv import find_dotenv, load_dotenv

from epc_connector import ingest
from epc_connector.ingest import (
    DEFAULT_CHUNK_ROWS,
    DEFAULT_ROW_GROUP_ROWS,
    HTTP_TIMEOUT,
    IngestError,
    archive_url_to_parquet,
    extract_remote_zip_members,
    merge_csvs_to_parquet,
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

SEARCH_URLS: Final[dict[EnglandWalesEpcDataset, str]] = {
    series: f"{API_URL}/api/{series}/search" for series in EnglandWalesEpcDataset
}
DELTAS_URL: Final = f"{API_URL}/api/deltas"
CERTIFICATE_URL: Final = f"{API_URL}/api/certificate"
SEARCH_PAGE_SIZE: Final = 5000  # API maximum

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
    return _certificates_year(name) is not None


def year_sort_key(path: Path) -> tuple[int, str]:
    """Sort key ordering ``certificates-YYYY.csv`` files oldest first."""
    return _certificates_year(path.name) or 0, path.name


def update_england_and_wales_epc(
    dataset: EnglandWalesEpcDataset | str,
    *,
    date_from: dt.date | str | None = None,
    date_to: dt.date | str | None = None,
    api_token: str | None = None,
    data_dir: Path | str = "data",
    staging_dir: Path | str = ".staging",
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
    row_group_rows: int = DEFAULT_ROW_GROUP_ROWS,
    schemas_dir: Path | str = DEFAULT_SCHEMAS_DIR,
    client: httpx.Client | None = None,
) -> Path:
    """
    Refresh a local Parquet file with certificates lodged in a date window.

    The search API (paginated) lists certificates lodged in the window and the
    deltas API lists certificates removed or whose UPRN changed. Full rows for
    the searched certificates are read from the window's yearly CSVs in the
    bulk ZIP via HTTP range requests, validated against the schema, and
    replace any local row with the same ``certificate_number``. Removed
    certificates are dropped. Local certificates whose UPRN changed get their
    ``uprn`` and ``uprn_source`` from the certificate API, one request each,
    so older yearly CSVs are never downloaded. Certificates found by search
    but not yet in the bulk file (regenerated monthly) are logged as pending;
    a later update adds them. Intermediate files are discarded.

    Parameters
    ----------
    dataset
        Series to update, e.g. ``"domestic"``.
    date_from
        First lodgement date to search (date or ``"YYYY-MM-DD"``). Defaults to
        the latest ``lodgement_date`` in the local file, ignoring future dates.
    date_to
        Last lodgement date to search; defaults to, and is capped at,
        yesterday (the API rejects ranges that include today).
    api_token, data_dir, staging_dir, chunk_rows, row_group_rows, schemas_dir, client
        As for ``download_england_and_wales_epc``.

    Returns
    -------
    Path
        Path of the updated Parquet file.

    Raises
    ------
    FileNotFoundError
        If there is no local file yet; run ``download_england_and_wales_epc``.
    ValueError
        If the window is empty or no start date can be found.
    """
    dataset = EnglandWalesEpcDataset(dataset)
    parquet_path = Path(data_dir) / PARQUET_NAMES[dataset]
    if not parquet_path.is_file():
        raise FileNotFoundError(
            f"{parquet_path} not found; run download_england_and_wales_epc first"
        )
    yesterday = dt.date.today() - dt.timedelta(days=1)
    start = _as_date(date_from) or latest_lodgement_date(parquet_path)
    if start is None:
        raise ValueError(f"no lodgement dates in {parquet_path}; pass date_from")
    end = min(_as_date(date_to) or yesterday, yesterday)
    if end < start:
        raise ValueError(f"empty window: {start} to {end}")
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
        searched = set(search_certificates(dataset, start, end, http))
        removed, uprn_updated = fetch_deltas(start, end, http)
        certificate = pl.col("certificate_number")
        local = pl.scan_parquet(parquet_path, low_memory=True)
        affected = set(
            local.filter(certificate.is_in(list(removed | uprn_updated)))
            .select(certificate)
            .collect()
            .to_series()
            .to_list()
        )
        wanted = searched - removed
        if not wanted and not affected:
            logger.info("%s: nothing new between %s and %s", dataset, start, end)
            return parquet_path

        found: set[str] = set()
        fresh = None
        last_updated = pl.read_parquet_metadata(parquet_path).get(
            LAST_UPDATED_METADATA_KEY, ""
        )
        if wanted:
            fresh_path = _fetch_window_rows(
                dataset, start, end, http, run_dir, schema, chunk_rows, row_group_rows
            )
            fresh = pl.scan_parquet(fresh_path, low_memory=True).filter(
                certificate.is_in(list(wanted | uprn_updated))
            )
            found = set(fresh.select(certificate).collect().to_series().to_list())
            last_updated = fetch_last_updated(dataset, http)

        uprns: dict[str, tuple[str, str]] = {}
        to_patch = sorted((affected & uprn_updated) - found)
        if to_patch:
            logger.info("Fetching %d changed UPRNs, one request each", len(to_patch))
        for number in to_patch:
            uprn = fetch_certificate_uprn(number, http)
            if uprn is not None:
                uprns[number] = uprn
        kept = local.filter(~certificate.is_in(list(found | removed)))
        if uprns:
            # A left join streams; replace_strict and when/then do not and
            # would load the whole file into memory.
            patch = pl.LazyFrame(
                [(number, *uprn) for number, uprn in uprns.items()],
                schema=["certificate_number", "new_uprn", "new_uprn_source"],
                orient="row",
            )
            kept = (
                kept.join(
                    patch, on="certificate_number", how="left", maintain_order="left"
                )
                .with_columns(
                    pl.coalesce("new_uprn", "uprn").alias("uprn"),
                    pl.coalesce("new_uprn_source", "uprn_source").alias("uprn_source"),
                )
                .drop("new_uprn", "new_uprn_source")
            )
        combined_path = run_dir / "combined.parquet"
        with pl.Config(streaming_chunk_size=chunk_rows):
            pl.concat([kept] if fresh is None else [kept, fresh]).sink_parquet(
                combined_path,
                compression="zstd",
                row_group_size=row_group_rows,
                metadata={LAST_UPDATED_METADATA_KEY: last_updated},
            )
        shutil.move(combined_path, parquet_path)

    logger.info(
        "Updated %s for %s to %s: %d added or refreshed, %d removed, "
        "%d UPRNs updated, %d pending (not yet in the bulk file)",
        parquet_path,
        start,
        end,
        len(found),
        len(removed & affected),
        len(uprns),
        len(wanted - found),
    )
    return parquet_path


def fetch_certificate_uprn(
    certificate_number: str, client: httpx.Client
) -> tuple[str, str] | None:
    """
    Return a certificate's current UPRN and its source from the certificate API.

    Returns
    -------
    tuple of str or None
        ``(uprn, uprn_source)`` as written in the bulk CSVs (``""`` for no
        UPRN), or None if the certificate is not published.
    """
    body = _get_json(
        client, CERTIFICATE_URL, {"certificate_number": certificate_number}
    )
    if body is None:
        return None
    document = body["data"]
    uprn = document.get("uprn")
    return ("" if uprn is None else str(uprn)), document.get("uprn_source") or ""


def search_certificates(
    dataset: EnglandWalesEpcDataset,
    date_from: dt.date,
    date_to: dt.date,
    client: httpx.Client,
    *,
    page_size: int = SEARCH_PAGE_SIZE,
) -> list[str]:
    """
    Return the certificate numbers lodged between two dates, inclusive.

    Pages through the search API ``page_size`` results at a time.
    """
    numbers: list[str] = []
    page: int | None = 1
    while page:
        body = _get_json(
            client,
            SEARCH_URLS[dataset],
            {
                "date_start": date_from.isoformat(),
                "date_end": date_to.isoformat(),
                "page_size": page_size,
                "current_page": page,
            },
        )
        if body is None:  # no certificates in the window
            break
        numbers.extend(item["certificateNumber"] for item in body["data"])
        pagination = body["pagination"]
        logger.info("Search page %s of %s", page, pagination.get("totalPages"))
        page = pagination.get("nextPage")
    return numbers


def fetch_deltas(
    date_from: dt.date, date_to: dt.date, client: httpx.Client
) -> tuple[set[str], set[str]]:
    """
    Return certificates removed, and those whose UPRN changed, between two dates.

    Returns
    -------
    tuple of set of str
        ``(removed, uprn_updated)`` certificate numbers, for all series.
    """
    body = _get_json(
        client,
        DELTAS_URL,
        {"date_start": date_from.isoformat(), "date_end": date_to.isoformat()},
    )
    events = body["data"] if body else []
    removed = {e["certificateNumber"] for e in events if e["eventType"] == "removed"}
    uprn_updated = {
        e["certificateNumber"] for e in events if e["eventType"] == "uprn_updated"
    }
    return removed, uprn_updated - removed


def latest_lodgement_date(
    parquet_path: Path, *, today: dt.date | None = None
) -> dt.date | None:
    """Return the latest ``lodgement_date``, ignoring dates after ``today``."""
    today = today or dt.date.today()
    lodged = pl.col("lodgement_date").str.to_date("%Y-%m-%d", strict=False)
    return (
        pl.scan_parquet(parquet_path, low_memory=True)
        .select(lodged.filter(lodged.le(today)).max())
        .collect()
        .item()
    )


def _fetch_window_rows(
    dataset: EnglandWalesEpcDataset,
    date_from: dt.date,
    date_to: dt.date,
    client: httpx.Client,
    run_dir: Path,
    schema: dict[str, Any],
    chunk_rows: int,
    row_group_rows: int,
) -> Path:
    """Merge the bulk ZIP's yearly CSVs for a date window into one Parquet file."""
    years = set(range(date_from.year, date_to.year + 1))
    csv_paths = extract_remote_zip_members(
        SOURCE_URLS[dataset],
        run_dir / "csv",
        client=client,
        include=lambda name: _certificates_year(name) in years,
    )
    if not csv_paths:
        raise IngestError(f"bulk file has no certificates for {sorted(years)}")
    fresh_path = run_dir / "fresh.parquet"
    merge_csvs_to_parquet(
        sorted(csv_paths, key=year_sort_key),
        fresh_path,
        chunk_rows=chunk_rows,
        row_group_rows=row_group_rows,
        schema=schema,
    )
    return fresh_path


def _get_json(
    client: httpx.Client, url: str, params: dict[str, Any]
) -> dict[str, Any] | None:
    """GET JSON, retrying when rate limited (429); ``None`` for "not found" (404)."""
    attempt = 1
    while True:
        response = client.get(url, params=params)
        if response.status_code == 404:
            return None
        if response.status_code != 429 or attempt == ingest.MAX_DOWNLOAD_ATTEMPTS:
            response.raise_for_status()
            return response.json()
        retry_after = response.headers.get("retry-after", "")
        delay = (
            float(retry_after)
            if retry_after.isdigit()
            else ingest.RETRY_BASE_DELAY_SECONDS * 2 ** (attempt - 1)
        )
        logger.warning("Rate limited by the API; retrying in %.0fs", delay)
        time.sleep(delay)
        attempt += 1


def _as_date(value: dt.date | str | None) -> dt.date | None:
    if isinstance(value, str):
        return dt.date.fromisoformat(value)
    return value


def _certificates_year(name: str) -> int | None:
    match = _CERTIFICATES_FILE.match(Path(name).name)
    return int(match[1]) if match else None
