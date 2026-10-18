"""
Tests for ``epc_connector.storage.parquet``.
"""

from pathlib import Path

import polars as pl
import pytest

from epc_connector.storage import parquet
from epc_connector.storage.files import StorageError
from tests.fakes import (
    SOURCE_URL,
    FakeServer,
    abc_schema,
    scottish_csv,
    yearly_members,
)


def write_csvs(directory: Path, members: dict[str, bytes]) -> dict[str, Path]:
    paths = {}
    for name, data in members.items():
        if name.endswith(".csv"):
            paths[name] = directory / name
            paths[name].write_bytes(data)
    return paths


class TestMergeCsvsToParquet:
    def test_values_are_kept_as_published_strings(self, tmp_path: Path) -> None:
        csvs = write_csvs(tmp_path, yearly_members())
        ordered = [csvs["2015.csv"], csvs["2016.csv"], csvs["2017.csv"]]
        output = tmp_path / "merged.parquet"

        row_count = parquet.merge_csvs_to_parquet(
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

        parquet.merge_csvs_to_parquet(ordered, output, skip_rows_after_header=1)

        assert pl.read_parquet(output)["A"].to_list() == ["1", "2", "3", "4"]

    def test_small_memory_limits_write_every_row(self, tmp_path: Path) -> None:
        rows = [f"{i},{i * 2},v{i}" for i in range(1_000)]
        csvs = write_csvs(tmp_path, {"2024.csv": scottish_csv(rows)})
        output = tmp_path / "merged.parquet"

        row_count = parquet.merge_csvs_to_parquet(
            list(csvs.values()),
            output,
            skip_rows_after_header=1,
            chunk_rows=64,
            row_group_rows=100,
        )

        assert row_count == 1_000
        assert pl.read_parquet(output)["A"].to_list() == [str(i) for i in range(1_000)]

    def test_changed_header_raises_storage_error(self, tmp_path: Path) -> None:
        members = {
            "2024.csv": scottish_csv(["1,2,3"]),
            "2025.csv": scottish_csv(["1,2,3"], header="A,B,RENAMED"),
        }
        csvs = write_csvs(tmp_path, members)

        with pytest.raises(StorageError, match="header differs"):
            parquet.merge_csvs_to_parquet(
                list(csvs.values()), tmp_path / "merged.parquet"
            )

    def test_empty_input_raises_storage_error(self, tmp_path: Path) -> None:
        with pytest.raises(StorageError, match="no CSV"):
            parquet.merge_csvs_to_parquet([], tmp_path / "merged.parquet")


class TestMergeCsvsToParquetWithSchema:
    def test_valid_data_is_written(self, tmp_path: Path) -> None:
        csvs = write_csvs(tmp_path, yearly_members())
        output = tmp_path / "merged.parquet"

        row_count = parquet.merge_csvs_to_parquet(
            list(csvs.values()), output, skip_rows_after_header=1, schema=abc_schema()
        )

        assert row_count == 4
        assert output.is_file()

    def test_header_mismatch_raises_before_writing(self, tmp_path: Path) -> None:
        csvs = write_csvs(tmp_path, {"2024.csv": scottish_csv(["1,2,3"], "A,B,D")})
        output = tmp_path / "merged.parquet"

        with pytest.raises(StorageError, match="header mismatch"):
            parquet.merge_csvs_to_parquet(
                list(csvs.values()), output, schema=abc_schema()
            )

        assert not output.exists()

    def test_invalid_values_raise_and_remove_output(self, tmp_path: Path) -> None:
        csvs = write_csvs(tmp_path, {"2024.csv": scottish_csv(["one,b,w"])})
        output = tmp_path / "merged.parquet"

        with pytest.raises(StorageError, match=r"A: 1 invalid") as error:
            parquet.merge_csvs_to_parquet(
                list(csvs.values()),
                output,
                skip_rows_after_header=1,
                schema=abc_schema(),
            )

        assert "C: 1 invalid" in str(error.value)
        assert not output.exists()


class TestStreamToParquet:
    def test_rows_and_metadata_are_written_in_row_groups(self, tmp_path: Path) -> None:
        output = tmp_path / "out.parquet"

        parquet.stream_to_parquet(
            pl.LazyFrame({"a": range(5)}),
            output,
            row_group_rows=2,
            metadata={"epc_connector.release": "r1"},
        )

        assert pl.read_parquet(output)["a"].to_list() == [0, 1, 2, 3, 4]
        assert pl.read_parquet_metadata(output)["epc_connector.release"] == "r1"


class TestReadCsvHeader:
    def test_utf8_bom_is_removed(self, tmp_path: Path) -> None:
        path = tmp_path / "2024.csv"
        path.write_bytes(scottish_csv([]))

        assert parquet.read_csv_header(path) == ["A", "B", "C"]


class TestArchiveUrlToParquet:
    def test_archive_is_downloaded_merged_and_deleted(
        self, yearly_archive: Path, tmp_path: Path
    ) -> None:
        server = FakeServer(yearly_archive.read_bytes())
        output = tmp_path / "merged.parquet"
        work_dir = tmp_path / "work"
        work_dir.mkdir()

        with server.client() as client:
            row_count = parquet.archive_url_to_parquet(
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
