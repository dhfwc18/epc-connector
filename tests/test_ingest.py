"""
Tests for ``epc_connector.ingest``.
"""

import lzma
import os
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest
from py7zr.exceptions import CrcError

from epc_connector import ingest
from epc_connector.ingest import IngestError
from tests.fakes import (
    SOURCE_URL,
    FakeServer,
    abc_schema,
    scottish_csv,
    write_7z,
    write_zip,
    yearly_members,
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
        self, yearly_archive: Path, tmp_path: Path
    ) -> None:
        destination = tmp_path / "out"

        paths = ingest.extract_files(yearly_archive, destination)

        assert sorted(p.name for p in paths) == ["2015.csv", "2016.csv", "2017.csv"]
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
        self, yearly_archive: Path, tmp_path: Path
    ) -> None:
        damaged = bytearray(yearly_archive.read_bytes())
        damaged[100] ^= 0xFF  # inside the compressed stream, after the headers
        yearly_archive.write_bytes(bytes(damaged))

        with pytest.raises((lzma.LZMAError, CrcError)):
            ingest.extract_files(yearly_archive, tmp_path / "out")


class TestExtractFilesDiskSpace:
    def test_insufficient_free_space_raises_before_extracting(
        self, yearly_archive: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("shutil.disk_usage", lambda path: SimpleNamespace(free=10))
        destination = tmp_path / "out"

        with pytest.raises(IngestError, match="GB is free"):
            ingest.extract_files(yearly_archive, destination)

        assert list(destination.iterdir()) == []


class TestMergeCsvsToParquet:
    def test_values_are_kept_as_published_strings(self, tmp_path: Path) -> None:
        csvs = write_csvs(tmp_path, yearly_members())
        ordered = [csvs["2015.csv"], csvs["2016.csv"], csvs["2017.csv"]]
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
        csvs = write_csvs(tmp_path, yearly_members())
        ordered = [csvs["2015.csv"], csvs["2016.csv"], csvs["2017.csv"]]
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


class TestMergeCsvsToParquetWithSchema:
    def test_valid_data_is_written(self, tmp_path: Path) -> None:
        csvs = write_csvs(tmp_path, yearly_members())
        output = tmp_path / "merged.parquet"

        row_count = ingest.merge_csvs_to_parquet(
            list(csvs.values()), output, skip_rows_after_header=1, schema=abc_schema()
        )

        assert row_count == 4
        assert output.is_file()

    def test_header_mismatch_raises_before_writing(self, tmp_path: Path) -> None:
        csvs = write_csvs(tmp_path, {"2024.csv": scottish_csv(["1,2,3"], "A,B,D")})
        output = tmp_path / "merged.parquet"

        with pytest.raises(IngestError, match="header mismatch"):
            ingest.merge_csvs_to_parquet(
                list(csvs.values()), output, schema=abc_schema()
            )

        assert not output.exists()

    def test_invalid_values_raise_and_remove_output(self, tmp_path: Path) -> None:
        csvs = write_csvs(tmp_path, {"2024.csv": scottish_csv(["one,b,w"])})
        output = tmp_path / "merged.parquet"

        with pytest.raises(IngestError, match=r"A: 1 invalid") as error:
            ingest.merge_csvs_to_parquet(
                list(csvs.values()),
                output,
                skip_rows_after_header=1,
                schema=abc_schema(),
            )

        assert "C: 1 invalid" in str(error.value)
        assert not output.exists()


class TestReadCsvHeader:
    def test_utf8_bom_is_removed(self, tmp_path: Path) -> None:
        path = tmp_path / "2024.csv"
        path.write_bytes(scottish_csv([]))

        assert ingest.read_csv_header(path) == ["A", "B", "C"]


class TestArchiveUrlToParquet:
    def test_archive_is_downloaded_merged_and_deleted(
        self, yearly_archive: Path, tmp_path: Path
    ) -> None:
        server = FakeServer(yearly_archive.read_bytes())
        output = tmp_path / "merged.parquet"
        work_dir = tmp_path / "work"
        work_dir.mkdir()

        with server.client() as client:
            row_count = ingest.archive_url_to_parquet(
                SOURCE_URL,
                output,
                work_dir=work_dir,
                client=client,
                sort_key=lambda path: path.name,
                skip_rows_after_header=1,
            )

        assert row_count == 4
        assert pl.read_parquet(output)["A"].to_list() == ["1", "2", "3", "4"]
        assert not (work_dir / "archive").exists()


class TestStagingDirectory:
    def test_run_directory_and_empty_staging_are_removed(self, tmp_path: Path) -> None:
        staging = tmp_path / ".staging"

        with ingest.staging_directory(staging, prefix="run-") as run_dir:
            (run_dir / "file.csv").write_text("a")
            assert run_dir.parent == staging

        assert not staging.exists()

    def test_staging_used_by_another_run_is_kept(self, tmp_path: Path) -> None:
        staging = tmp_path / ".staging"
        staging.mkdir()
        (staging / "other-run").mkdir()

        with ingest.staging_directory(staging, prefix="run-") as run_dir:
            pass

        assert not run_dir.exists()
        assert staging.is_dir()

    def test_run_directory_is_removed_after_an_error(self, tmp_path: Path) -> None:
        staging = tmp_path / ".staging"

        with pytest.raises(RuntimeError), ingest.staging_directory(staging, "run-"):
            raise RuntimeError("boom")

        assert not staging.exists()
