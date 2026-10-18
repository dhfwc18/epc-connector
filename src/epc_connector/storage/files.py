"""
Local file handling that keeps memory and disk use bounded.

Gigabytes of files pass through each step, and the kernel keeps every byte
read or written in its page cache. A WSL virtual machine does not hand that
memory back to Windows promptly, so large files are written through
``StreamingFile`` and dropped from the cache with ``drop_page_cache`` once
processed. Files are built in a ``staging_directory`` and moved into place
with ``atomic_write``, so a failed run never leaves a half-written file.
"""

import contextlib
import logging
import os
import shutil
import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import IO, Final, Self

from epc_connector.errors import EpcConnectorError

logger = logging.getLogger(__name__)

STREAM_BLOCK_BYTES: Final = 1024 * 1024
"""Bytes read from a download or archive member at a time."""
PAGE_CACHE_DROP_BYTES: Final = 64 * 1024 * 1024
"""Bytes written between page cache drops."""


class StorageError(EpcConnectorError, RuntimeError):
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


@contextlib.contextmanager
def staging_directory(staging_dir: Path, prefix: str) -> Generator[Path]:
    """
    Yield a fresh directory inside ``staging_dir`` and delete it afterwards.

    ``staging_dir`` itself is removed too once no other run is using it.
    """
    staging_dir.mkdir(parents=True, exist_ok=True)
    try:
        with tempfile.TemporaryDirectory(prefix=prefix, dir=staging_dir) as run_dir:
            logger.debug("Working in %s", run_dir)
            yield Path(run_dir)
    finally:
        with contextlib.suppress(OSError):
            staging_dir.rmdir()


@contextlib.contextmanager
def atomic_write(path: Path, work_dir: Path) -> Generator[Path]:
    """
    Yield a temporary path in ``work_dir``; move it to ``path`` on success.

    ``path`` is only replaced once the block finishes without an error, so
    readers never see a half-written file. On an error the temporary file is
    left in ``work_dir`` (normally a ``staging_directory``) to be deleted with
    it.

    Parameters
    ----------
    path
        Final file; its directory is created if missing.
    work_dir
        Directory for the temporary file, on the same disk as ``path`` if
        possible so the move is a rename.
    """
    temporary = work_dir / f"{path.name}.partial"
    yield temporary
    path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(temporary, path)
    logger.debug("Moved the new %s into place", path)


def check_free_space(directory: Path, needed_bytes: int) -> None:
    """
    Raise unless ``directory`` has ``needed_bytes`` of free space.

    Raises
    ------
    StorageError
        If there is not enough free space.
    """
    free_bytes = shutil.disk_usage(directory).free
    if needed_bytes > free_bytes:
        raise StorageError(
            f"needs {needed_bytes / 1e9:.1f} GB but only "
            f"{free_bytes / 1e9:.1f} GB is free under {directory}"
        )
