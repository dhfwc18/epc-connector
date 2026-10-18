"""
Extract selected files from 7z and zip archives, local or remote.

Members are written straight to disk through ``StreamingFile``, so neither
memory nor the page cache grows with the archive's size.
"""

import contextlib
import io
import logging
import shutil
import zipfile
from collections.abc import Callable
from pathlib import Path

import httpx
import py7zr
from py7zr.io import Py7zIO, WriterFactory

from epc_connector.storage.downloads import RemoteFile, resolve_download_url
from epc_connector.storage.files import (
    STREAM_BLOCK_BYTES,
    StorageError,
    StreamingFile,
    check_free_space,
)

logger = logging.getLogger(__name__)


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
    StorageError
        If the archive is neither 7z nor zip, holds no matching files, or the
        matching files would not fit in the free space under ``destination``.
    """
    destination.mkdir(parents=True, exist_ok=True)
    logger.debug("Extracting %s files from %s", suffix, archive_path)
    if py7zr.is_7zfile(archive_path):
        with py7zr.SevenZipFile(archive_path) as archive:
            sizes = {i.filename: i.uncompressed for i in archive.list()}
            names = _select(list(sizes), suffix, include)
            check_free_space(destination, sum(sizes[name] for name in names))
            if names:
                with contextlib.closing(_StreamingWriterFactory()) as factory:
                    archive.extract(path=destination, targets=names, factory=factory)
    elif zipfile.is_zipfile(archive_path):
        with zipfile.ZipFile(archive_path) as archive:
            sizes = {i.filename: i.file_size for i in archive.infolist()}
            names = _select(list(sizes), suffix, include)
            check_free_space(destination, sum(sizes[name] for name in names))
            for name in names:
                target = destination / name
                target.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(name) as source, StreamingFile(target) as output:
                    shutil.copyfileobj(source, output, STREAM_BLOCK_BYTES)
    else:
        raise StorageError(f"{archive_path.name} is neither a 7z nor a zip archive")
    if not names:
        raise StorageError(f"no {suffix} files in {archive_path.name}")
    logger.debug("Extracted %d files: %s", len(names), ", ".join(names))
    return [destination / name for name in names]


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
    StorageError
        If the members would not fit in the free space under ``destination``.
    """
    destination.mkdir(parents=True, exist_ok=True)
    file_url, size = resolve_download_url(url, client)
    remote = io.BufferedReader(
        RemoteFile(file_url, size, client), buffer_size=read_block_bytes
    )
    with remote, zipfile.ZipFile(remote) as archive:
        members = [m for m in archive.infolist() if include(m.filename)]
        check_free_space(destination, sum(m.file_size for m in members))
        paths = []
        for member in members:
            path = destination / Path(member.filename).name
            with archive.open(member) as source, StreamingFile(path) as target:
                shutil.copyfileobj(source, target, read_block_bytes)
            paths.append(path)
            logger.debug("Fetched %s (%d bytes)", member.filename, member.file_size)
    return paths


def _select(
    names: list[str], suffix: str, include: Callable[[str], bool] | None
) -> list[str]:
    return [
        name
        for name in names
        if name.lower().endswith(suffix.lower()) and (include is None or include(name))
    ]


class _StreamingWriter(Py7zIO):
    """py7zr writer that extracts a member to disk via ``StreamingFile``."""

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
    """
    Creates ``_StreamingWriter`` objects and closes them all on ``close``.

    py7zr leaves the member being written open when extraction fails.
    """

    def __init__(self) -> None:
        self._writers: list[_StreamingWriter] = []

    def create(self, filename: str) -> Py7zIO:
        writer = _StreamingWriter(Path(filename))
        self._writers.append(writer)
        return writer

    def close(self) -> None:
        for writer in self._writers:
            writer.close()
