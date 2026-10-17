"""
Download, unpack and convert raw source files with bounded memory.

- ``download_file`` streams a URL to disk in fixed-size blocks and resumes
  with a ``Range`` request after a dropped connection.
- ``extract_files`` pulls selected files out of a 7z or zip archive.
- ``merge_csvs_to_parquet`` streams same-header CSVs into one Parquet file
  with Polars' streaming engine, with explicit chunk and row-group limits.
- ``archive_url_to_parquet`` chains the three: one archive URL in, one
  validated Parquet file out.
- ``staging_directory`` provides a self-cleaning working directory.
- ``extract_remote_zip_members`` reads selected members of a remote ZIP with
  HTTP range requests, without downloading the whole archive.
"""

import contextlib
import csv
import io
import logging
import os
import re
import shutil
import tempfile
import time
import zipfile
from collections.abc import Callable, Generator, Mapping, Sequence
from pathlib import Path
from typing import IO, Any, Final, Self

import httpx
import polars as pl
import py7zr
from py7zr.io import Py7zIO, WriterFactory

from epc_connector import validation

logger = logging.getLogger(__name__)

HTTP_TIMEOUT: Final = httpx.Timeout(30.0, read=120.0)
STREAM_BLOCK_BYTES: Final = 1024 * 1024
MAX_DOWNLOAD_ATTEMPTS: Final = 5
RETRY_BASE_DELAY_SECONDS: Final = 1.0
DEFAULT_CHUNK_ROWS: Final = 10_000
DEFAULT_ROW_GROUP_ROWS: Final = 50_000

# Bytes written between page cache drops.
PAGE_CACHE_DROP_BYTES: Final = 64 * 1024 * 1024

_CONTENT_RANGE_TOTAL: Final = re.compile(r"/(\d+)$")


class IngestError(RuntimeError):
    """A download, archive or CSV is not as expected."""


def drop_page_cache(path: Path) -> None:
    """
    Write a file's cached pages to disk and ask the kernel to drop them.

    Does nothing where ``os.posix_fadvise`` is unavailable (not Linux).
    """
    if not hasattr(os, "posix_fadvise"):
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
        os.posix_fadvise(descriptor, 0, 0, os.POSIX_FADV_DONTNEED)
    finally:
        os.close(descriptor)


class StreamingFile:
    """
    A binary file for large sequential writes that keeps the page cache small.

    Every ``PAGE_CACHE_DROP_BYTES`` written, and on close, the written pages
    are flushed to disk and dropped from the cache.

    Parameters
    ----------
    path
        File to open.
    mode
        ``"wb"``, ``"ab"`` or ``"w+b"``.
    """

    def __init__(self, path: Path, mode: str = "wb") -> None:
        self._file: IO[bytes] = path.open(mode)
        self._unflushed = 0

    def write(self, data: bytes | bytearray | memoryview) -> int:
        written = self._file.write(data)
        self._unflushed += written
        if self._unflushed >= PAGE_CACHE_DROP_BYTES:
            self._drop_cache()
        return written

    def read(self, size: int = -1) -> bytes:
        return self._file.read(size)

    def seek(self, offset: int, whence: int = 0) -> int:
        return self._file.seek(offset, whence)

    def tell(self) -> int:
        return self._file.tell()

    def flush(self) -> None:
        self._file.flush()

    def close(self) -> None:
        if not self._file.closed:
            self._drop_cache()
            self._file.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def _drop_cache(self) -> None:
        self._file.flush()
        if hasattr(os, "posix_fadvise"):
            descriptor = self._file.fileno()
            os.fdatasync(descriptor)
            os.posix_fadvise(descriptor, 0, 0, os.POSIX_FADV_DONTNEED)
        self._unflushed = 0


class _StreamingWriter(Py7zIO):
    """py7zr writer that extracts a member straight to disk via ``StreamingFile``."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._file = StreamingFile(path, "w+b")
        self._size = 0

    def write(self, s: bytes | bytearray) -> int:
        written = self._file.write(s)
        self._size += written
        return written

    def read(self, size: int | None = None) -> bytes:
        return self._file.read(-1 if size is None else size)

    def seek(self, offset: int, whence: int = 0) -> int:
        return self._file.seek(offset, whence)

    def flush(self) -> None:
        self._file.flush()

    def size(self) -> int:
        return self._size

    def close(self) -> None:
        self._file.close()


class _StreamingWriterFactory(WriterFactory):
    def create(self, filename: str) -> Py7zIO:
        return _StreamingWriter(Path(filename))


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
                    with StreamingFile(
                        destination, "ab" if written else "wb"
                    ) as output:
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
    archive_path: Path,
    destination: Path,
    *,
    suffix: str = ".csv",
    include: Callable[[str], bool] | None = None,
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
    include
        Optional extra filter on member names; only names it accepts are
        extracted.

    Returns
    -------
    list of Path
        Paths of the extracted files, in archive order.

    Raises
    ------
    IngestError
        If the archive is neither 7z nor zip, holds no matching files, or the
        matching files would not fit in the free space under ``destination``.
    """
    destination.mkdir(parents=True, exist_ok=True)
    if py7zr.is_7zfile(archive_path):
        with py7zr.SevenZipFile(archive_path) as archive:
            sizes = {i.filename: i.uncompressed for i in archive.list()}
            names = _select(list(sizes), suffix, include)
            _check_free_space(destination, sum(sizes[name] for name in names))
            if names:
                archive.extract(
                    path=destination, targets=names, factory=_StreamingWriterFactory()
                )
    elif zipfile.is_zipfile(archive_path):
        with zipfile.ZipFile(archive_path) as archive:
            sizes = {i.filename: i.file_size for i in archive.infolist()}
            names = _select(list(sizes), suffix, include)
            _check_free_space(destination, sum(sizes[name] for name in names))
            for name in names:
                target = destination / name
                target.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(name) as source, StreamingFile(target) as output:
                    shutil.copyfileobj(source, output, STREAM_BLOCK_BYTES)
    else:
        raise IngestError(f"{archive_path.name} is neither a 7z nor a zip archive")
    if not names:
        raise IngestError(f"no {suffix} files in {archive_path.name}")
    return [destination / name for name in names]


def _check_free_space(directory: Path, needed_bytes: int) -> None:
    free_bytes = shutil.disk_usage(directory).free
    if needed_bytes > free_bytes:
        raise IngestError(
            f"extracting needs {needed_bytes / 1e9:.1f} GB but only "
            f"{free_bytes / 1e9:.1f} GB is free under {directory}"
        )


def _select(
    names: list[str], suffix: str, include: Callable[[str], bool] | None
) -> list[str]:
    return [
        name
        for name in names
        if name.lower().endswith(suffix.lower()) and (include is None or include(name))
    ]


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
        Source JSON Schema describing one row (see ``epc_connector.validation``).
    metadata
        Key-value pairs stored in the Parquet file's metadata.

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
            raise IngestError(
                "data breaks the schema:\n" + validation.format_violations(violations)
            )
    for csv_path in csv_paths:  # read in full; nothing else needs them cached
        drop_page_cache(csv_path)
    rows = pl.scan_parquet(output_path).select(pl.len())
    return rows.collect(engine="streaming").item()


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


def read_csv_header(csv_path: Path) -> list[str]:
    """Return the column names in the first row of a CSV (UTF-8 BOM removed)."""
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
        Archive URL (7z or zip); see ``download_file``.
    output_path
        Parquet file to write.
    work_dir
        Scratch directory, e.g. from ``staging_directory``.
    client
        HTTP client for the download.
    include
        Member-name filter; see ``extract_files``.
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
    download_file(url, archive_path, client=client)
    csv_paths = extract_files(archive_path, work_dir / "csv", include=include)
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


@contextlib.contextmanager
def staging_directory(staging_dir: Path, prefix: str) -> Generator[Path]:
    """
    Yield a fresh directory inside ``staging_dir`` and delete it afterwards.

    ``staging_dir`` itself is removed too once no other run is using it.
    """
    staging_dir.mkdir(parents=True, exist_ok=True)
    try:
        with tempfile.TemporaryDirectory(prefix=prefix, dir=staging_dir) as run_dir:
            yield Path(run_dir)
    finally:
        with contextlib.suppress(OSError):
            staging_dir.rmdir()


def resolve_download_url(url: str, client: httpx.Client) -> tuple[str, int]:
    """
    Follow redirects from ``url`` and return the final URL and file size.

    Uses a one-byte range request, so no body beyond one byte is read.

    Raises
    ------
    IngestError
        If the final server does not support range requests.
    """
    with client.stream(
        "GET", url, headers={"Range": "bytes=0-0"}, follow_redirects=True
    ) as response:
        response.raise_for_status()
        total = _total_bytes(response, 0)
        if response.status_code != 206 or total is None:
            raise IngestError(f"{url} does not support range requests")
        return str(response.url), total


class RemoteFile(io.RawIOBase):
    """
    Read-only, seekable view of a remote file using HTTP range requests.

    Requests carry no ``Authorization`` header, as pre-signed URLs reject
    them. Wrap in ``io.BufferedReader`` to avoid many tiny requests.

    Parameters
    ----------
    url
        Final file URL, e.g. a pre-signed S3 URL from ``resolve_download_url``.
    size
        File size in bytes.
    client
        HTTP client used for the range requests.
    """

    def __init__(self, url: str, size: int, client: httpx.Client) -> None:
        super().__init__()
        self._url = url
        self._size = size
        self._client = client
        self._position = 0

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._position

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        base = {io.SEEK_SET: 0, io.SEEK_CUR: self._position, io.SEEK_END: self._size}
        self._position = max(0, base[whence] + offset)
        return self._position

    def readinto(self, buffer: Any) -> int:
        end = min(self._position + len(buffer), self._size) - 1
        if end < self._position:
            return 0
        data = self._get_range(self._position, end)
        buffer[: len(data)] = data
        self._position += len(data)
        return len(data)

    def _get_range(self, start: int, end: int) -> bytes:
        attempt = 1
        while True:
            request = self._client.build_request(
                "GET", self._url, headers={"Range": f"bytes={start}-{end}"}
            )
            request.headers.pop("Authorization", None)
            try:
                response = self._client.send(request)
                response.raise_for_status()
                if response.status_code != 206:
                    raise IngestError(f"no partial response for bytes {start}-{end}")
                return response.content
            except (httpx.HTTPError, IngestError) as error:
                if attempt == MAX_DOWNLOAD_ATTEMPTS:
                    raise IngestError(f"range {start}-{end} failed: {error}") from error
                delay = RETRY_BASE_DELAY_SECONDS * 2 ** (attempt - 1)
                logger.warning("%s; retrying in %.0fs", error, delay)
                time.sleep(delay)
                attempt += 1


def extract_remote_zip_members(
    url: str,
    destination: Path,
    *,
    client: httpx.Client,
    include: Callable[[str], bool],
    read_block_bytes: int = 8 * 1024 * 1024,
) -> list[Path]:
    """
    Extract selected members of a remote ZIP without downloading the rest.

    Only the ZIP index and the selected members are fetched, using HTTP range
    requests.

    Parameters
    ----------
    url
        ZIP URL; redirects are followed (e.g. API -> pre-signed S3 URL).
    destination
        Directory to write the members into; created if missing.
    client
        HTTP client; its ``Authorization`` header is not sent to the file URL.
    include
        Filter on member names; only names it accepts are extracted.
    read_block_bytes
        Size of each range request.

    Returns
    -------
    list of Path
        Paths of the extracted members, in archive order (an empty list if
        none match).

    Raises
    ------
    IngestError
        If the members would not fit in the free space under ``destination``.
    """
    destination.mkdir(parents=True, exist_ok=True)
    file_url, size = resolve_download_url(url, client)
    remote = io.BufferedReader(
        RemoteFile(file_url, size, client), buffer_size=read_block_bytes
    )
    with remote, zipfile.ZipFile(remote) as archive:
        members = [m for m in archive.infolist() if include(m.filename)]
        _check_free_space(destination, sum(m.file_size for m in members))
        paths = []
        for member in members:
            path = destination / Path(member.filename).name
            with archive.open(member) as source, StreamingFile(path) as target:
                shutil.copyfileobj(source, target, read_block_bytes)
            paths.append(path)
            logger.info("Fetched %s (%d bytes)", member.filename, member.file_size)
    return paths
