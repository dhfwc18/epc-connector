"""
Tests for ``epc_connector.io.downloads``.
"""

import io
import os
from pathlib import Path

import httpx
import pytest

from epc_connector.io import downloads
from epc_connector.io.files import StorageError
from tests.fakes import (
    PRESIGNED_URL,
    SOURCE_URL,
    FakeServer,
)


class TestDownloadFile:
    def test_redirected_url_downloads_complete_file(self, tmp_path: Path) -> None:
        payload = os.urandom(100_000)
        server = FakeServer(payload)
        destination = tmp_path / "archive.7z"

        with server.client() as client:
            size = downloads.download_file(SOURCE_URL, destination, client=client)

        assert size == len(payload)
        assert destination.read_bytes() == payload
        assert server.ranges == [None]

    def test_dropped_connection_resumes_from_last_written_byte(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            downloads, "STREAM_BLOCK_BYTES", 10_000
        )  # flush before drop
        payload = os.urandom(100_000)
        server = FakeServer(payload, drop_after=40_000)
        destination = tmp_path / "archive.7z"

        with server.client() as client:
            downloads.download_file(SOURCE_URL, destination, client=client)

        assert destination.read_bytes() == payload
        assert server.ranges == [None, "bytes=40000-"]
        assert server.calls["redirect"] == 2  # fresh presigned URL for the resume

    def test_ignored_range_header_restarts_from_beginning(self, tmp_path: Path) -> None:
        payload = os.urandom(100_000)
        server = FakeServer(payload, drop_after=40_000, supports_ranges=False)
        destination = tmp_path / "archive.7z"

        with server.client() as client:
            downloads.download_file(SOURCE_URL, destination, client=client)

        assert destination.read_bytes() == payload

    def test_persistent_failure_raises_storage_error(self, tmp_path: Path) -> None:
        server = FakeServer(os.urandom(1_000), drop_after=10)
        destination = tmp_path / "archive.7z"

        with server.client() as client, pytest.raises(StorageError, match="failed"):
            downloads.download_file(SOURCE_URL, destination, client=client, attempts=1)

    def test_zero_attempts_raises_value_error(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="attempts"):
            downloads.download_file(SOURCE_URL, tmp_path / "archive.7z", attempts=0)


class TestResolveDownloadUrl:
    def test_redirect_is_followed_and_size_reported(self) -> None:
        payload = os.urandom(5_000)
        server = FakeServer(payload)

        with server.client() as client:
            url, size = downloads.resolve_download_url(SOURCE_URL, client)

        assert url == PRESIGNED_URL
        assert size == len(payload)

    def test_server_without_ranges_raises_storage_error(self) -> None:
        server = FakeServer(os.urandom(100), supports_ranges=False)

        with server.client() as client, pytest.raises(StorageError, match="range"):
            downloads.resolve_download_url(SOURCE_URL, client)


class TestRemoteFile:
    def test_seek_and_read_return_the_requested_bytes(self) -> None:
        payload = os.urandom(10_000)
        server = FakeServer(payload)

        with server.client() as client:
            remote = downloads.RemoteFile(PRESIGNED_URL, len(payload), client)
            remote.seek(2_500)
            middle = remote.read(1_000)
            remote.seek(-100, io.SEEK_END)
            tail = remote.read()

        assert middle == payload[2_500:3_500]
        assert tail == payload[-100:]

    def test_authorization_header_is_not_sent(self) -> None:
        payload = os.urandom(1_000)
        server = FakeServer(payload)
        client = httpx.Client(
            transport=server.transport(), headers={"Authorization": "Bearer secret"}
        )

        with client:
            data = downloads.RemoteFile(PRESIGNED_URL, len(payload), client).read()

        assert data == payload
        assert server.calls["file_with_authorization"] == 0
