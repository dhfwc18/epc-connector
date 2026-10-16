"""
Download, unpack and convert raw source files with bounded memory.

- ``download_file`` streams a URL to disk in fixed-size blocks and resumes
  with a ``Range`` request after a dropped connection.
- ``extract_files`` pulls selected files out of a 7z or zip archive.
- ``merge_csvs_to_parquet`` streams same-header CSVs into one Parquet file
  with Polars' streaming engine, with explicit chunk and row-group limits.
"""

import contextlib
import csv
import logging
import re
import time
import zipfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Final

import httpx
import polars as pl
import py7zr

from epc_connector import validation

logger = logging.getLogger(__name__)

HTTP_TIMEOUT: Final = httpx.Timeout(30.0, read=120.0)
STREAM_BLOCK_BYTES: Final = 1024 * 1024
MAX_DOWNLOAD_ATTEMPTS: Final = 5
RETRY_BASE_DELAY_SECONDS: Final = 1.0
DEFAULT_CHUNK_ROWS: Final = 10_000
DEFAULT_ROW_GROUP_ROWS: Final = 50_000

_CONTENT_RANGE_TOTAL: Final = re.compile(r"/(\d+)$")


class IngestError(RuntimeError):
    """A download, archive or CSV is not as expected."""


def download_file(
    url: str,
    destination: Path,
    *,
    client: httpx.Client | None = None,
    attempts: int = MAX_DOWNLOAD_ATTEMPTS,
) -> int:
    """
    Stream ``url`` to ``destination``, resuming after dropped connections.

    Parameters
    ----------
    url
        File URL; redirects are followed on every attempt, so an expired
        presigned URL is replaced by a fresh one.
    destination
        File to write; created or overwritten.
    client
        HTTP client to use; a temporary one is created if omitted.
    attempts
        Maximum number of requests before giving up.

    Returns
    -------
    int
        Size of the file in bytes.

    Raises
    ------
    IngestError
        If the file is still incomplete after ``attempts`` requests.
    """
    if attempts < 1:
        raise ValueError("attempts must be at least 1")
    with contextlib.ExitStack() as stack:
        if client is None:
            client = stack.enter_context(httpx.Client(timeout=HTTP_TIMEOUT))
        written = 0
        for attempt in range(1, attempts + 1):
            headers = {"Range": f"bytes={written}-"} if written else {}
            try:
                with client.stream(
                    "GET", url, headers=headers, follow_redirects=True
                ) as response:
                    response.raise_for_status()
                    if response.status_code != 206:
                        written = 0  # full response: (re)start from the beginning
                    total = _total_bytes(response, written)
                    with destination.open("ab" if written else "wb") as output:
                        for block in response.iter_bytes(STREAM_BLOCK_BYTES):
                            output.write(block)
                            written += len(block)
                if total is None or written == total:
                    return written
                raise IngestError(f"received {written} of {total} bytes")
            except (httpx.HTTPError, IngestError) as error:
                if attempt == attempts:
                    raise IngestError(f"download of {url} failed: {error}") from error
                delay = RETRY_BASE_DELAY_SECONDS * 2 ** (attempt - 1)
                logger.warning(
                    "%s; resuming at byte %d in %.0fs", error, written, delay
                )
                time.sleep(delay)
    raise AssertionError("unreachable")


def _total_bytes(response: httpx.Response, already_written: int) -> int | None:
    """Full file size from ``Content-Range`` or ``Content-Length``, if known."""
    if response.status_code == 206:
        match = _CONTENT_RANGE_TOTAL.search(response.headers.get("content-range", ""))
        return int(match[1]) if match else None
    length = response.headers.get("content-length")
    return int(length) + already_written if length else None


def extract_files(
    archive_path: Path, destination: Path, *, suffix: str = ".csv"
) -> list[Path]:
    """
    Extract the files whose names end with ``suffix`` from a 7z or zip archive.

    Parameters
    ----------
    archive_path
        A 7z or zip archive; the format is detected from its content.
    destination
        Directory to extract into; created if missing.
    suffix
        File name ending to select, compared case-insensitively.

    Returns
    -------
    list of Path
        Paths of the extracted files, in archive order.

    Raises
    ------
    IngestError
        If the archive is neither 7z nor zip, or holds no matching files.
    """
    destination.mkdir(parents=True, exist_ok=True)
    if py7zr.is_7zfile(archive_path):
        with py7zr.SevenZipFile(archive_path) as archive:
            names = _with_suffix(archive.getnames(), suffix)
            if names:
                archive.extract(path=destination, targets=names)
    elif zipfile.is_zipfile(archive_path):
        with zipfile.ZipFile(archive_path) as archive:
            names = _with_suffix(archive.namelist(), suffix)
            for name in names:
                archive.extract(name, destination)
    else:
        raise IngestError(f"{archive_path.name} is neither a 7z nor a zip archive")
    if not names:
        raise IngestError(f"no {suffix} files in {archive_path.name}")
    return [destination / name for name in names]


def _with_suffix(names: list[str], suffix: str) -> list[str]:
    return [name for name in names if name.lower().endswith(suffix.lower())]


def merge_csvs_to_parquet(
    csv_paths: Sequence[Path],
    output_path: Path,
    *,
    skip_rows_after_header: int = 0,
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
    row_group_rows: int = DEFAULT_ROW_GROUP_ROWS,
    schema: Mapping[str, Any] | None = None,
) -> int:
    """
    Stream CSV files with the same header into one Parquet file.

    With ``schema``, the header must match the schema's columns before
    anything is written, and every value is checked against the schema once
    the file is written; a file that fails is deleted.

    Values are kept as published: every column is a string and empty fields
    stay empty strings. Rows are written in the order of ``csv_paths``. Peak
    memory scales with ``chunk_rows`` times the Polars thread count (set
    ``POLARS_MAX_THREADS`` to lower it) plus one buffered row group.

    Parameters
    ----------
    csv_paths
        CSV files to merge, in output order.
    output_path
        Parquet file to write (zstd compressed).
    skip_rows_after_header
        Rows to drop after the header of every file, e.g. a label row.
    chunk_rows
        Rows each thread processes at a time.
    row_group_rows
        Rows per Parquet row group, i.e. rows buffered before each write.
    schema
        Source JSON Schema describing one row (see ``epc_connector.validation``).

    Returns
    -------
    int
        Number of rows written.

    Raises
    ------
    IngestError
        If there are no files, their headers differ, or the data breaks
        ``schema``.
    """
    if not csv_paths:
        raise IngestError("no CSV files to merge")
    header = read_csv_header(csv_paths[0])
    for csv_path in csv_paths[1:]:
        if read_csv_header(csv_path) != header:
            raise IngestError(
                f"{csv_path.name} header differs from {csv_paths[0].name}"
            )
    if schema is not None:
        try:
            validation.check_header(header, schema)
        except ValueError as error:
            raise IngestError(f"{csv_paths[0].name}: {error}") from error

    frames = [
        pl.scan_csv(
            csv_path,
            skip_rows_after_header=skip_rows_after_header,
            infer_schema=False,
            empty_string_is_null=False,
            low_memory=True,
        )
        for csv_path in csv_paths
    ]
    with pl.Config(streaming_chunk_size=chunk_rows):
        pl.concat(frames).sink_parquet(
            output_path, compression="zstd", row_group_size=row_group_rows
        )
        if schema is not None:
            written = pl.scan_parquet(output_path, low_memory=True)
            violations = validation.find_violations(written, schema)
            if violations:
                output_path.unlink()
                raise IngestError(
                    "data breaks the schema:\n"
                    + validation.format_violations(violations)
                )
    return pl.scan_parquet(output_path).select(pl.len()).collect().item()


def read_csv_header(csv_path: Path) -> list[str]:
    """Return the column names in the first row of a CSV (UTF-8 BOM removed)."""
    with csv_path.open(encoding="utf-8-sig", newline="") as csv_file:
        return next(csv.reader(csv_file))
