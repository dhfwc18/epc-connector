"""
Turn CSV files into Parquet, and write Parquet, with bounded memory.

``merge_csvs_to_parquet`` streams same-header CSVs into one Parquet file with
Polars' streaming engine, with explicit chunk and row-group limits.
``archive_url_to_parquet`` chains a download, an extraction and that merge.
"""

import csv
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, Final

import httpx
import polars as pl

from epc_connector import validation
from epc_connector.storage import archives, downloads
from epc_connector.storage.files import StorageError, drop_page_cache

DEFAULT_CHUNK_ROWS: Final = 10_000
"""Rows each Polars thread processes at a time."""
DEFAULT_ROW_GROUP_ROWS: Final = 50_000
"""Rows per Parquet row group (rows buffered before each write)."""


def stream_to_parquet(
    lf: pl.LazyFrame,
    output_path: Path,
    *,
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
    row_group_rows: int = DEFAULT_ROW_GROUP_ROWS,
    metadata: Mapping[str, str] | None = None,
) -> None:
    """
    Stream a lazy frame to a zstd-compressed Parquet file with bounded memory.

    The written file is dropped from the page cache afterwards.

    Parameters
    ----------
    lf
        Frame to write.
    output_path
        Parquet file to write.
    chunk_rows
        Rows each thread processes at a time.
    row_group_rows
        Rows per Parquet row group, i.e. rows buffered before each write.
    metadata
        Key-value pairs stored in the Parquet file's metadata.
    """
    with pl.Config(streaming_chunk_size=chunk_rows):
        lf.sink_parquet(
            output_path,
            compression="zstd",
            row_group_size=row_group_rows,
            metadata=dict(metadata) if metadata else None,
        )
    drop_page_cache(output_path)


def merge_csvs_to_parquet(
    csv_paths: Sequence[Path],
    output_path: Path,
    *,
    skip_rows_after_header: int = 0,
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
    row_group_rows: int = DEFAULT_ROW_GROUP_ROWS,
    schema: Mapping[str, Any] | None = None,
    metadata: Mapping[str, str] | None = None,
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
        Source JSON Schema of one row (see ``epc_connector.validation``).
    metadata
        Key-value pairs stored in the Parquet file's metadata.

    Returns
    -------
    int
        Number of rows written.

    Raises
    ------
    StorageError
        If there are no files, their headers differ, or the data breaks
        ``schema``.
    """
    if not csv_paths:
        raise StorageError("no CSV files to merge")
    header = read_csv_header(csv_paths[0])
    for csv_path in csv_paths[1:]:
        if read_csv_header(csv_path) != header:
            raise StorageError(
                f"{csv_path.name} header differs from {csv_paths[0].name}"
            )
    if schema is not None:
        try:
            validation.check_header(header, schema)
        except ValueError as error:
            raise StorageError(f"{csv_paths[0].name}: {error}") from error

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
    stream_to_parquet(
        pl.concat(frames),
        output_path,
        chunk_rows=chunk_rows,
        row_group_rows=row_group_rows,
        metadata=metadata,
    )
    if schema is not None:
        written = pl.scan_parquet(output_path, low_memory=True)
        with pl.Config(streaming_chunk_size=chunk_rows):
            violations = validation.find_violations(written, schema)
        if violations:
            output_path.unlink()
            raise StorageError(
                "data breaks the schema:\n" + validation.format_violations(violations)
            )
    for csv_path in csv_paths:  # read in full; nothing else needs them cached
        drop_page_cache(csv_path)
    rows = pl.scan_parquet(output_path).select(pl.len())
    return rows.collect(engine="streaming").item()


def read_csv_header(csv_path: Path) -> list[str]:
    """Return the column names in a CSV's first row (UTF-8 BOM removed)."""
    with csv_path.open(encoding="utf-8-sig", newline="") as csv_file:
        return next(csv.reader(csv_file))


def archive_url_to_parquet(
    url: str,
    output_path: Path,
    *,
    work_dir: Path,
    client: httpx.Client,
    include: Callable[[str], bool] | None = None,
    sort_key: Callable[[Path], Any] | None = None,
    skip_rows_after_header: int = 0,
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
    row_group_rows: int = DEFAULT_ROW_GROUP_ROWS,
    schema: Mapping[str, Any] | None = None,
    metadata: Mapping[str, str] | None = None,
) -> int:
    """
    Download an archive of CSVs and merge them into one validated Parquet file.

    The archive and extracted CSVs are written to ``work_dir``; the archive is
    deleted before the merge to free disk.

    Parameters
    ----------
    url
        Archive URL (7z or zip); see ``downloads.download_file``.
    output_path
        Parquet file to write.
    work_dir
        Scratch directory, e.g. from ``files.staging_directory``.
    client
        HTTP client for the download.
    include
        Member-name filter; see ``archives.extract_files``.
    sort_key
        Order of the CSVs in the output; archive order if omitted.
    skip_rows_after_header, chunk_rows, row_group_rows, schema, metadata
        As for ``merge_csvs_to_parquet``.

    Returns
    -------
    int
        Number of rows written.
    """
    archive_path = work_dir / "archive"
    downloads.download_file(url, archive_path, client=client)
    csv_paths = archives.extract_files(archive_path, work_dir / "csv", include=include)
    archive_path.unlink()
    return merge_csvs_to_parquet(
        sorted(csv_paths, key=sort_key) if sort_key else csv_paths,
        output_path,
        skip_rows_after_header=skip_rows_after_header,
        chunk_rows=chunk_rows,
        row_group_rows=row_group_rows,
        schema=schema,
        metadata=metadata,
    )
