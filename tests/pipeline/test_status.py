"""
Tests for ``epc_connector._pipeline.status``.
"""

from pathlib import Path

import polars as pl

from epc_connector._canonical import GB_PARQUET_NAME
from epc_connector._datasets import RELEASE_METADATA_KEY, Origin
from epc_connector._pipeline import status
from tests.fakes import write_raw_domestic


class TestStatusLines:
    def test_empty_data_dir_marks_nothing_set_up(self, tmp_path: Path) -> None:
        lines = status.status_lines(tmp_path)

        assert not any(status.SET_UP in line for line in lines)
        assert sum(status.MISSING in line for line in lines) == 8

    def test_present_dataset_is_marked_with_its_release(self, tmp_path: Path) -> None:
        write_raw_domestic(
            tmp_path, Origin.SCOTLAND, [{}], {RELEASE_METADATA_KEY: "2026Q2"}
        )

        [line] = [
            line for line in status.status_lines(tmp_path) if status.SET_UP in line
        ]

        assert "scotland/domestic" in line
        assert line.endswith("(2026Q2)")

    def test_gb_domestic_is_marked_when_present(self, tmp_path: Path) -> None:
        pl.DataFrame({"a": [1]}).write_parquet(tmp_path / GB_PARQUET_NAME)

        lines = status.status_lines(tmp_path)

        assert any(status.SET_UP in line and "gb/domestic" in line for line in lines)

    def test_gb_without_a_file_yet_says_not_set_up(self, tmp_path: Path) -> None:
        lines = status.status_lines(tmp_path)

        for name in ["gb/non-domestic", "gb/display"]:
            [line] = [line for line in lines if name in line]
            assert line.endswith("not set up yet")

    def test_report_is_ascii(self, tmp_path: Path) -> None:
        assert "\n".join(status.status_lines(tmp_path)).isascii()
