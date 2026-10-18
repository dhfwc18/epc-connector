"""
Download England and Wales EPC open data, one Parquet file per segment.

Each segment is a bulk ZIP from Get energy performance of buildings data,
regenerated monthly: one ``certificates-YYYY.csv`` per year of
``lodgement_date``, plus ``recommendations-YYYY.csv`` files for some segments,
which are skipped. The API needs a bearer token, taken from ``EPB_API_TOKEN``
unless passed explicitly. Values stay as published strings and are checked
against ``schemas/source/england-and-wales/<segment>.schema.json``. The file's
``lastUpdated`` timestamp is stored in the Parquet metadata.

``update`` refreshes a local file from the search and
deltas APIs, reading only the window's yearly CSVs from the bulk ZIP and
patching changed UPRNs from the certificate API.
"""

import contextlib
import datetime as dt
import logging
import os
import re
import time
from pathlib import Path
from typing import Any, Final

import httpx
import polars as pl
from dotenv import find_dotenv, load_dotenv

from epc_connector.datasets import (
    LAST_UPDATED_METADATA_KEY,
    Origin,
    Segment,
    get_dataset,
    select_datasets,
)
from epc_connector.sources._shared import as_date, client_or_new
from epc_connector.storage import (
    DEFAULT_CHUNK_ROWS,
    DEFAULT_ROW_GROUP_ROWS,
    HTTP_TIMEOUT,
    StorageError,
    archive_url_to_parquet,
    atomic_write,
    downloads,
    extract_remote_zip_members,
    merge_csvs_to_parquet,
    staging_directory,
    stream_to_parquet,
)
from epc_connector.validation import DEFAULT_SCHEMAS_DIR, load_schema

logger = logging.getLogger(__name__)


API_URL: Final = "https://api.get-energy-performance-data.communities.gov.uk"
"""Base URL of the Get energy performance of buildings data API."""
DOMESTIC_URL: Final = f"{API_URL}/api/files/domestic/csv"
"""Bulk CSV ZIP of domestic certificates."""
NON_DOMESTIC_URL: Final = f"{API_URL}/api/files/non-domestic/csv"
"""Bulk CSV ZIP of non-domestic certificates."""
DISPLAY_URL: Final = f"{API_URL}/api/files/display/csv"
"""Bulk CSV ZIP of Display Energy Certificates."""
SOURCE_URLS: Final[dict[Segment, str]] = {
    Segment.DOMESTIC: DOMESTIC_URL,
    Segment.NON_DOMESTIC: NON_DOMESTIC_URL,
    Segment.DISPLAY: DISPLAY_URL,
}
"""Bulk CSV ZIP URL, by segment."""
SEARCH_URLS: Final[dict[Segment, str]] = {
    dataset.segment: f"{API_URL}/api/{dataset.segment}/search"
    for dataset in select_datasets([Origin.ENGLAND_AND_WALES])
}
"""Certificate search endpoint, by segment."""
DELTAS_URL: Final = f"{API_URL}/api/deltas"
"""Endpoint listing removed certificates and changed UPRNs."""
CERTIFICATE_URL: Final = f"{API_URL}/api/certificate"
"""Endpoint returning one certificate as JSON."""
SEARCH_PAGE_SIZE: Final = 5000
"""Results per search page (the API maximum)."""

API_TOKEN_ENV_VAR: Final = "EPB_API_TOKEN"
"""Environment variable holding the API bearer token."""

_CERTIFICATES_FILE: Final = re.compile(r"^certificates-(\d{4})\.csv$")


def download(
    segment: Segment | str,
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
    Download an England and Wales EPC segment into one Parquet file.

    Column names are checked against the schema before writing and every value
    before the file is moved into ``data_dir``. An existing file is replaced
    only once the new one is complete and valid.

    Parameters
    ----------
    segment
        Segment to download: ``"domestic"``, ``"non-domestic"`` or
        ``"display"``.
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
        Directory holding ``source/england-and-wales/<segment>.schema.json``.
    client
        Authorised HTTP client to use, e.g. from ``authorised_client``.

    Returns
    -------
    Path
        Path of the Parquet file, e.g.
        ``data/domestic_epc_certificate_england_wales.parquet``.

    Raises
    ------
    DatasetError
        If ``segment`` is not a segment.
    """
    dataset = get_dataset(Origin.ENGLAND_AND_WALES, segment)
    segment = dataset.segment
    parquet_path = dataset.path(data_dir)
    schema = load_schema(dataset.source_schema(schemas_dir))
    with (
        _api_client(client, api_token) as http,
        staging_directory(Path(staging_dir), prefix=f"ew-{segment}-") as run_dir,
    ):
        last_updated = fetch_last_updated(segment, http)
        logger.info("Downloading %s (published %s)", dataset.name, last_updated)
        with atomic_write(parquet_path, run_dir) as merged_path:
            row_count = archive_url_to_parquet(
                SOURCE_URLS[segment],
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


def fetch_last_updated(segment: Segment, client: httpx.Client) -> str:
    """Return when the segment's bulk file was last rebuilt (ISO timestamp)."""
    response = client.get(f"{SOURCE_URLS[segment]}/info")
    response.raise_for_status()
    return response.json()["data"]["lastUpdated"]


def is_certificates_file(name: str) -> bool:
    """True for ``certificates-YYYY.csv`` members (not recommendations)."""
    return _certificates_year(name) is not None


def year_sort_key(path: Path) -> tuple[int, str]:
    """Sort key ordering ``certificates-YYYY.csv`` files oldest first."""
    return _certificates_year(path.name) or 0, path.name


def update(
    segment: Segment | str,
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
    segment
        Segment to update: ``"domestic"``, ``"non-domestic"`` or
        ``"display"``.
    date_from
        First lodgement date to search (date or ``"YYYY-MM-DD"``). Defaults to
        the latest ``lodgement_date`` in the local file, ignoring future dates.
    date_to
        Last lodgement date to search; defaults to, and is capped at,
        yesterday (the API rejects ranges that include today).
    api_token, data_dir, staging_dir, chunk_rows, row_group_rows
        As for ``download``.
    schemas_dir, client
        As for ``download``.

    Returns
    -------
    Path
        Path of the updated Parquet file.

    Raises
    ------
    FileNotFoundError
        If there is no local file yet; run ``download`` first.
    ValueError
        If the window is empty or no start date can be found.
    """
    dataset = get_dataset(Origin.ENGLAND_AND_WALES, segment)
    segment = dataset.segment
    parquet_path = dataset.path(data_dir)
    if not parquet_path.is_file():
        raise FileNotFoundError(f"{parquet_path} not found; download it first")
    yesterday = dt.date.today() - dt.timedelta(days=1)
    start = as_date(date_from) or latest_lodgement_date(parquet_path)
    if start is None:
        raise ValueError(f"no lodgement dates in {parquet_path}; pass date_from")
    end = min(as_date(date_to) or yesterday, yesterday)
    if end < start:
        raise ValueError(f"empty window: {start} to {end}")
    schema = load_schema(dataset.source_schema(schemas_dir))

    with (
        _api_client(client, api_token) as http,
        staging_directory(Path(staging_dir), prefix=f"ew-{segment}-") as run_dir,
    ):
        searched = set(search_certificates(segment, start, end, http))
        removed, uprn_updated = fetch_deltas(start, end, http)
        logger.debug(
            "%s from %s to %s: %d lodged, %d removed, %d UPRNs changed",
            dataset.name,
            start,
            end,
            len(searched),
            len(removed),
            len(uprn_updated),
        )
        certificate = pl.col("certificate_number")
        local = pl.scan_parquet(parquet_path, low_memory=True)
        affected = set(
            local.filter(certificate.is_in(list(removed | uprn_updated)))
            .select(certificate)
            .collect(engine="streaming")
            .to_series()
            .to_list()
        )
        wanted = searched - removed
        if not wanted and not affected:
            logger.info("%s: nothing new between %s and %s", dataset.name, start, end)
            return parquet_path

        found: set[str] = set()
        fresh = None
        last_updated = pl.read_parquet_metadata(parquet_path).get(
            LAST_UPDATED_METADATA_KEY, ""
        )
        if wanted:
            fresh_path = _fetch_window_rows(
                segment, start, end, http, run_dir, schema, chunk_rows, row_group_rows
            )
            fresh = pl.scan_parquet(fresh_path, low_memory=True).filter(
                certificate.is_in(list(wanted | uprn_updated))
            )
            found = set(
                fresh.select(certificate)
                .collect(engine="streaming")
                .to_series()
                .to_list()
            )
            last_updated = fetch_last_updated(segment, http)

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
        with atomic_write(parquet_path, run_dir) as combined_path:
            stream_to_parquet(
                pl.concat([kept] if fresh is None else [kept, fresh]),
                combined_path,
                chunk_rows=chunk_rows,
                row_group_rows=row_group_rows,
                metadata={LAST_UPDATED_METADATA_KEY: last_updated},
            )

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
    Return a certificate's UPRN and the UPRN's source, from the API.

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
    segment: Segment,
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
            SEARCH_URLS[segment],
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
        logger.debug("Search page %s of %s", page, pagination.get("totalPages"))
        page = pagination.get("nextPage")
    return numbers


def fetch_deltas(
    date_from: dt.date, date_to: dt.date, client: httpx.Client
) -> tuple[set[str], set[str]]:
    """
    Return the certificates removed, or whose UPRN changed, between two dates.

    Returns
    -------
    tuple of set of str
        ``(removed, uprn_updated)`` certificate numbers, for all segments.
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
        .collect(engine="streaming")
        .item()
    )


def _fetch_window_rows(
    segment: Segment,
    date_from: dt.date,
    date_to: dt.date,
    client: httpx.Client,
    run_dir: Path,
    schema: dict[str, Any],
    chunk_rows: int,
    row_group_rows: int,
) -> Path:
    """Merge the bulk ZIP's yearly CSVs for a date window into one file."""
    years = set(range(date_from.year, date_to.year + 1))
    csv_paths = extract_remote_zip_members(
        SOURCE_URLS[segment],
        run_dir / "csv",
        client=client,
        include=lambda name: _certificates_year(name) in years,
    )
    if not csv_paths:
        raise StorageError(f"bulk file has no certificates for {sorted(years)}")
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
    """GET JSON, retrying when rate limited (429); ``None`` for 404."""
    attempt = 1
    while True:
        response = client.get(url, params=params)
        if response.status_code == 404:
            return None
        if response.status_code != 429 or attempt == downloads.MAX_DOWNLOAD_ATTEMPTS:
            response.raise_for_status()
            return response.json()
        retry_after = response.headers.get("retry-after", "")
        delay = (
            float(retry_after)
            if retry_after.isdigit()
            else downloads.RETRY_BASE_DELAY_SECONDS * 2 ** (attempt - 1)
        )
        logger.warning("Rate limited by the API; retrying in %.0fs", delay)
        time.sleep(delay)
        attempt += 1


def _api_client(
    client: httpx.Client | None, api_token: str | None
) -> contextlib.AbstractContextManager[httpx.Client]:
    """The given client, or a new authorised one closed after use."""
    return client_or_new(
        client, lambda: authorised_client(api_token, timeout=HTTP_TIMEOUT)
    )


def _certificates_year(name: str) -> int | None:
    match = _CERTIFICATES_FILE.match(Path(name).name)
    return int(match[1]) if match else None
