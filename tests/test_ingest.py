"""
Tests for ``epc_connector.ingest``.
"""

import lzma
import os
from pathlib import Path

import polars as pl
import pytest
from py7zr.exceptions import CrcError

from epc_connector import ingest
from epc_connector.ingest import IngestError
from tests.fakes import (
    SOURCE_URL,
    FakeServer,
    quarterly_members,
    scottish_csv,
    write_7z,
    write_zip,
)


def write_csvs(directory: Path, members: dict[str, bytes]) -> dict[str, Path]:
    paths = {}
    for name, data in members.items():
        if name.endswith(".csv"):
            paths[name] = directory / name
            paths[name].write_bytes(data)
    return paths


class TestDownloadFile:
    def test_redirected_url_downloads_complete_file(self, tmp_path: Path) -> None:
        payload = os.urandom(100_000)
        server = FakeServer(payload)
        destination = tmp_path / "archive.7z"

        with server.client() as client:
            size = ingest.download_file(SOURCE_URL, destination, client=client)

        assert size == len(payload)
        assert destination.read_bytes() == payload
        assert server.ranges == [None]

    def test_dropped_connection_resumes_from_last_written_byte(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(ingest, "STREAM_BLOCK_BYTES", 10_000)  # flush before drop
        payload = os.urandom(100_000)
        server = FakeServer(payload, drop_after=40_000)
        destination = tmp_path / "archive.7z"

        with server.client() as client:
            ingest.download_file(SOURCE_URL, destination, client=client)

        assert destination.read_bytes() == payload
        assert server.ranges == [None, "bytes=40000-"]
        assert server.calls["redirect"] == 2  # fresh presigned URL for the resume

    def test_ignored_range_header_restarts_from_beginning(self, tmp_path: Path) -> None:
        payload = os.urandom(100_000)
        server = FakeServer(payload, drop_after=40_000, supports_ranges=False)
        destination = tmp_path / "archive.7z"

        with server.client() as client:
            ingest.download_file(SOURCE_URL, destination, client=client)

        assert destination.read_bytes() == payload

    def test_persistent_failure_raises_ingest_error(self, tmp_path: Path) -> None:
        server = FakeServer(os.urandom(1_000), drop_after=10)
        destination = tmp_path / "archive.7z"

        with server.client() as client, pytest.raises(IngestError, match="failed"):
            ingest.download_file(SOURCE_URL, destination, client=client, attempts=1)

    def test_zero_attempts_raises_value_error(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="attempts"):
            ingest.download_file(SOURCE_URL, tmp_path / "archive.7z", attempts=0)


class TestExtractFiles:
    def test_7z_archive_extracts_only_matching_files(
        self, quarterly_archive: Path, tmp_path: Path
    ) -> None:
        destination = tmp_path / "out"

        paths = ingest.extract_files(quarterly_archive, destination)

        assert sorted(p.name for p in paths) == [
            "Q1 2017.csv",
            "Q3 2016.csv",
            "Q4 2016.csv",
        ]
        assert all(p.is_file() for p in paths)
        assert not list(destination.glob("*.pdf"))

    def test_zip_archive_extracts_only_matching_files(self, tmp_path: Path) -> None:
        members = {"2024.csv": b"A\r\n1\r\n", "readme.txt": b"notes"}
        archive = write_zip(tmp_path / "archive.zip", members)

        paths = ingest.extract_files(archive, tmp_path / "out")

        assert [p.name for p in paths] == ["2024.csv"]

    def test_archive_without_matches_raises_ingest_error(self, tmp_path: Path) -> None:
        archive = write_7z(tmp_path / "archive.7z", {"note.pdf": b"%PDF"})

        with pytest.raises(IngestError, match=r"no \.csv files"):
            ingest.extract_files(archive, tmp_path / "out")

    def test_unknown_format_raises_ingest_error(self, tmp_path: Path) -> None:
        not_an_archive = tmp_path / "archive.7z"
        not_an_archive.write_bytes(b"plain text")

        with pytest.raises(IngestError, match="neither"):
            ingest.extract_files(not_an_archive, tmp_path / "out")

    def test_corrupt_7z_raises_decompression_error(
        self, quarterly_archive: Path, tmp_path: Path
    ) -> None:
        damaged = bytearray(quarterly_archive.read_bytes())
        damaged[100] ^= 0xFF  # inside the compressed stream, after the headers
        quarterly_archive.write_bytes(bytes(damaged))

        with pytest.raises((lzma.LZMAError, CrcError)):
            ingest.extract_files(quarterly_archive, tmp_path / "out")


class TestMergeCsvsToParquet:
    def test_values_are_kept_as_published_strings(self, tmp_path: Path) -> None:
        csvs = write_csvs(tmp_path, quarterly_members())
        ordered = [csvs["Q3 2016.csv"], csvs["Q4 2016.csv"], csvs["Q1 2017.csv"]]
        output = tmp_path / "merged.parquet"

        row_count = ingest.merge_csvs_to_parquet(
            ordered, output, skip_rows_after_header=1
        )

        frame = pl.read_parquet(output)
        assert row_count == 4
        assert frame.columns == ["A", "B", "C"]
        assert all(dtype == pl.String for dtype in frame.dtypes)
        assert frame["B"].to_list() == ["10.5", "0", "multi\r\nline", ""]
        assert frame["C"].to_list() == ["x", "y", "z", ""]

    def test_rows_follow_input_file_order(self, tmp_path: Path) -> None:
        csvs = write_csvs(tmp_path, quarterly_members())
        ordered = [csvs["Q3 2016.csv"], csvs["Q4 2016.csv"], csvs["Q1 2017.csv"]]
        output = tmp_path / "merged.parquet"

        ingest.merge_csvs_to_parquet(ordered, output, skip_rows_after_header=1)

        assert pl.read_parquet(output)["A"].to_list() == ["1", "2", "3", "4"]

    def test_small_memory_limits_write_every_row(self, tmp_path: Path) -> None:
        rows = [f"{i},{i * 2},v{i}" for i in range(1_000)]
        csvs = write_csvs(tmp_path, {"2024.csv": scottish_csv(rows)})
        output = tmp_path / "merged.parquet"

        row_count = ingest.merge_csvs_to_parquet(
            list(csvs.values()),
            output,
            skip_rows_after_header=1,
            chunk_rows=64,
            row_group_rows=100,
        )

        assert row_count == 1_000
        assert pl.read_parquet(output)["A"].to_list() == [str(i) for i in range(1_000)]

    def test_changed_header_raises_ingest_error(self, tmp_path: Path) -> None:
        members = {
            "2024.csv": scottish_csv(["1,2,3"]),
            "2025.csv": scottish_csv(["1,2,3"], header="A,B,RENAMED"),
        }
        csvs = write_csvs(tmp_path, members)

        with pytest.raises(IngestError, match="header differs"):
            ingest.merge_csvs_to_parquet(
                list(csvs.values()), tmp_path / "merged.parquet"
            )

    def test_empty_input_raises_ingest_error(self, tmp_path: Path) -> None:
        with pytest.raises(IngestError, match="no CSV"):
            ingest.merge_csvs_to_parquet([], tmp_path / "merged.parquet")


class TestReadCsvHeader:
    def test_utf8_bom_is_removed(self, tmp_path: Path) -> None:
        path = tmp_path / "2024.csv"
        path.write_bytes(scottish_csv([]))

        assert ingest.read_csv_header(path) == ["A", "B", "C"]
