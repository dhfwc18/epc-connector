"""
Download files over HTTP with bounded memory.

``download_file`` streams a URL to disk in fixed-size blocks and resumes with
a ``Range`` request after a dropped connection. ``RemoteFile`` reads parts of
a remote file with range requests, without downloading the rest.
"""

import contextlib
import io
import logging
import re
import time
from pathlib import Path
from typing import Any, Final

import httpx

from epc_connector.storage.files import STREAM_BLOCK_BYTES, StorageError, StreamingFile

logger = logging.getLogger(__name__)

HTTP_TIMEOUT: Final = httpx.Timeout(30.0, read=120.0)
"""Connect timeout of 30 s; 120 s to wait for each block of a download."""
MAX_DOWNLOAD_ATTEMPTS: Final = 5
"""Requests made for one file before giving up."""
RETRY_BASE_DELAY_SECONDS: Final = 1.0
"""Wait before the first retry; doubled for each further retry."""

_CONTENT_RANGE_TOTAL: Final = re.compile(r"/(\d+)$")


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
    StorageError
        If the file is still incomplete after ``attempts`` requests.
    """
    if attempts < 1:
        raise ValueError("attempts must be at least 1")
    logger.debug("Downloading %s to %s", url, destination)
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
                raise StorageError(f"received {written} of {total} bytes")
            except (httpx.HTTPError, StorageError) as error:
                if attempt == attempts:
                    raise StorageError(f"download of {url} failed: {error}") from error
                delay = RETRY_BASE_DELAY_SECONDS * 2 ** (attempt - 1)
                logger.warning(
                    "%s; resuming at byte %d in %.0fs", error, written, delay
                )
                time.sleep(delay)
    raise AssertionError("unreachable")


def _total_bytes(response: httpx.Response, already_written: int) -> int | None:
    """File size from ``Content-Range`` or ``Content-Length``, if known."""
    if response.status_code == 206:
        match = _CONTENT_RANGE_TOTAL.search(response.headers.get("content-range", ""))
        return int(match[1]) if match else None
    length = response.headers.get("content-length")
    return int(length) + already_written if length else None


def resolve_download_url(url: str, client: httpx.Client) -> tuple[str, int]:
    """
    Follow redirects from ``url`` and return the final URL and file size.

    Uses a one-byte range request, so no body beyond one byte is read.

    Raises
    ------
    StorageError
        If the final server does not support range requests.
    """
    with client.stream(
        "GET", url, headers={"Range": "bytes=0-0"}, follow_redirects=True
    ) as response:
        response.raise_for_status()
        total = _total_bytes(response, 0)
        if response.status_code != 206 or total is None:
            raise StorageError(f"{url} does not support range requests")
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
                    raise StorageError(f"no partial response for bytes {start}-{end}")
                return response.content
            except (httpx.HTTPError, StorageError) as error:
                if attempt == MAX_DOWNLOAD_ATTEMPTS:
                    raise StorageError(
                        f"range {start}-{end} failed: {error}"
                    ) from error
                delay = RETRY_BASE_DELAY_SECONDS * 2 ** (attempt - 1)
                logger.warning("%s; retrying in %.0fs", error, delay)
                time.sleep(delay)
                attempt += 1
