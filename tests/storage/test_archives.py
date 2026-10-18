"""
Tests for ``epc_connector.storage.archives``.
"""

import lzma
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from py7zr.exceptions import CrcError

from epc_connector.storage import archives
from epc_connector.storage.files import StorageError
from tests.fakes import (
    SOURCE_URL,
    FakeServer,
    write_7z,
    write_zip,
)


class TestExtractFiles:
    def test_7z_archive_extracts_only_matching_files(
        self, yearly_archive: Path, tmp_path: Path
    ) -> None:
        destination = tmp_path / "out"

        paths = archives.extract_files(yearly_archive, destination)

        assert sorted(p.name for p in paths) == ["2015.csv", "2016.csv", "2017.csv"]
        assert all(p.is_file() for p in paths)
        assert not list(destination.glob("*.pdf"))

    def test_zip_archive_extracts_only_matching_files(self, tmp_path: Path) -> None:
        members = {"2024.csv": b"A\r\n1\r\n", "readme.txt": b"notes"}
        archive = write_zip(tmp_path / "archive.zip", members)

        paths = archives.extract_files(archive, tmp_path / "out")

        assert [p.name for p in paths] == ["2024.csv"]

    def test_archive_without_matches_raises_storage_error(self, tmp_path: Path) -> None:
        archive = write_7z(tmp_path / "archive.7z", {"note.pdf": b"%PDF"})

        with pytest.raises(StorageError, match=r"no \.csv files"):
            archives.extract_files(archive, tmp_path / "out")

    def test_unknown_format_raises_storage_error(self, tmp_path: Path) -> None:
        not_an_archive = tmp_path / "archive.7z"
        not_an_archive.write_bytes(b"plain text")

        with pytest.raises(StorageError, match="neither"):
            archives.extract_files(not_an_archive, tmp_path / "out")

    def test_corrupt_7z_raises_decompression_error(
        self, yearly_archive: Path, tmp_path: Path
    ) -> None:
        damaged = bytearray(yearly_archive.read_bytes())
        damaged[100] ^= 0xFF  # inside the compressed stream, after the headers
        yearly_archive.write_bytes(bytes(damaged))

        with pytest.raises((lzma.LZMAError, CrcError)):
            archives.extract_files(yearly_archive, tmp_path / "out")


class TestExtractFilesDiskSpace:
    def test_insufficient_free_space_raises_before_extracting(
        self, yearly_archive: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("shutil.disk_usage", lambda path: SimpleNamespace(free=10))
        destination = tmp_path / "out"

        with pytest.raises(StorageError, match="GB is free"):
            archives.extract_files(yearly_archive, destination)

        assert list(destination.iterdir()) == []


class TestExtractRemoteZipMembers:
    def test_only_selected_members_are_fetched(self, tmp_path: Path) -> None:
        filler = os.urandom(400_000)  # incompressible, must not be downloaded
        members = {"certificates-2010.csv": filler, "certificates-2024.csv": b"a\n1\n"}
        archive = write_zip(tmp_path / "bulk.zip", members)
        server = FakeServer(archive.read_bytes())

        with server.client() as client:
            paths = archives.extract_remote_zip_members(
                SOURCE_URL,
                tmp_path / "out",
                client=client,
                include=lambda name: name.endswith("2024.csv"),
                read_block_bytes=16 * 1024,
            )

        assert [p.name for p in paths] == ["certificates-2024.csv"]
        assert paths[0].read_bytes() == b"a\n1\n"
        assert server.bytes_served < len(filler) // 4

    def test_no_matching_members_returns_empty_list(self, tmp_path: Path) -> None:
        archive = write_zip(tmp_path / "bulk.zip", {"certificates-2024.csv": b"a\n"})
        server = FakeServer(archive.read_bytes())

        with server.client() as client:
            paths = archives.extract_remote_zip_members(
                SOURCE_URL, tmp_path / "out", client=client, include=lambda name: False
            )

        assert paths == []
