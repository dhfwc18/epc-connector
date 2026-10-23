"""
Tests for ``epc_connector._canonical.gb``.
"""

import datetime as dt
import logging
from pathlib import Path

import polars as pl
import pytest

from epc_connector._canonical import gb
from epc_connector._canonical.mapping import MappingError, canonical_dtypes
from epc_connector._datasets import (
    LAST_UPDATED_METADATA_KEY,
    RELEASE_METADATA_KEY,
    Origin,
    Segment,
    get_dataset,
)
from tests.fakes import ENGLAND_AND_WALES_ROW, SCOTLAND_ROW, write_raw_domestic


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    data_dir = tmp_path / "data"
    write_raw_domestic(
        data_dir,
        Origin.ENGLAND_AND_WALES,
        [ENGLAND_AND_WALES_ROW],
        {LAST_UPDATED_METADATA_KEY: "2026-10-01"},
    )
    write_raw_domestic(
        data_dir, Origin.SCOTLAND, [SCOTLAND_ROW], {RELEASE_METADATA_KEY: "2026q2"}
    )
    return data_dir


def build_into(data_dir: Path, tmp_path: Path) -> Path:
    return gb.build_gb(data_dir=data_dir, staging_dir=tmp_path / ".staging")


class TestBuildGb:
    def test_file_has_the_canonical_columns_and_types(
        self, data_dir: Path, tmp_path: Path
    ) -> None:
        output = build_into(data_dir, tmp_path)

        assert output == data_dir / "domestic_epc_certificate_gb.parquet"
        assert dict(pl.read_parquet_schema(output)) == canonical_dtypes()

    def test_both_origins_are_mapped_and_stacked_in_order(
        self, data_dir: Path, tmp_path: Path
    ) -> None:
        wales, scotland = pl.read_parquet(build_into(data_dir, tmp_path)).iter_rows(
            named=True
        )

        assert wales["country"] == "wales"
        assert wales["uprn"] == 100023336956
        assert wales["lodged_at"] == dt.datetime(2024, 3, 1, 10)
        assert wales["construction_year_min"] == 1930
        assert wales["tenure"] == "rented (private)"
        assert wales["total_floor_area"] is None
        assert wales["heating_cost_current"] == 300.0
        assert wales["walls_insulation"] == "filled cavity"
        assert wales["walls_energy_efficiency"] == "good"
        assert wales["main_heating_fuel"] == "electric"
        assert wales["superseded"] is None
        assert scotland["country"] == "scotland"
        assert scotland["superseded"] is True
        assert scotland["construction_year_max"] is None
        assert scotland["heating_cost_current"] == 100.0
        assert scotland["walls_construction"] == "cavity wall"
        assert scotland["walls_parts"] == 2

    def test_provenance_of_both_raw_files_is_kept(
        self, data_dir: Path, tmp_path: Path
    ) -> None:
        stored = pl.read_parquet_metadata(build_into(data_dir, tmp_path))

        assert stored[LAST_UPDATED_METADATA_KEY] == "2026-10-01"
        assert stored[RELEASE_METADATA_KEY] == "2026q2"
        assert not (tmp_path / ".staging").exists()

    def test_unmapped_value_stops_before_writing(
        self, data_dir: Path, tmp_path: Path
    ) -> None:
        row = SCOTLAND_ROW | {"CONSTRUCTION_AGE_BAND": "1700s"}
        write_raw_domestic(data_dir, Origin.SCOTLAND, [row], {})

        with pytest.raises(MappingError, match="1700s"):
            build_into(data_dir, tmp_path)

        assert not (data_dir / gb.GB_PARQUET_NAME).exists()

    def test_missing_origin_is_left_out_and_logged(
        self, data_dir: Path, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        get_dataset(Origin.SCOTLAND, Segment.DOMESTIC).path(data_dir).unlink()

        with caplog.at_level(logging.INFO):
            frame = pl.read_parquet(build_into(data_dir, tmp_path))

        assert frame["source_dataset"].to_list() == ["england-and-wales/domestic"]
        assert "scotland/domestic is not downloaded" in caplog.text

    def test_no_raw_files_raises_file_not_found_error(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match="no raw domestic files"):
            build_into(tmp_path, tmp_path)


def heating_costs(path: Path) -> dict[str, float]:
    frame = pl.read_parquet(path)
    return dict(
        zip(frame["source_dataset"], frame["heating_cost_current"], strict=True)
    )


class TestReplaceOriginInGb:
    def test_only_the_changed_origin_is_cleaned_again(
        self, data_dir: Path, tmp_path: Path
    ) -> None:
        build_into(data_dir, tmp_path)
        # Both raw files change, but only Scotland's rows are rebuilt.
        write_raw_domestic(
            data_dir,
            Origin.ENGLAND_AND_WALES,
            [ENGLAND_AND_WALES_ROW | {"heating_cost_current": "900"}],
            {LAST_UPDATED_METADATA_KEY: "2026-11-01"},
        )
        write_raw_domestic(
            data_dir,
            Origin.SCOTLAND,
            [SCOTLAND_ROW | {"HEATING_COST_CURRENT": "600"}],
            {RELEASE_METADATA_KEY: "2026q3"},
        )

        output = gb.replace_origin_in_gb(
            "scotland", data_dir=data_dir, staging_dir=tmp_path / ".staging"
        )

        assert heating_costs(output) == {
            "england-and-wales/domestic": 300.0,
            "scotland/domestic": 200.0,
        }
        stored = pl.read_parquet_metadata(output)
        assert stored[LAST_UPDATED_METADATA_KEY] == "2026-10-01"
        assert stored[RELEASE_METADATA_KEY] == "2026q3"

    def test_origin_order_is_kept(self, data_dir: Path, tmp_path: Path) -> None:
        build_into(data_dir, tmp_path)

        output = gb.replace_origin_in_gb(
            "england-and-wales", data_dir=data_dir, staging_dir=tmp_path / ".staging"
        )

        assert pl.read_parquet(output)["source_dataset"].to_list() == [
            "england-and-wales/domestic",
            "scotland/domestic",
        ]

    def test_without_a_gb_file_it_is_built_from_every_raw_file(
        self, data_dir: Path, tmp_path: Path
    ) -> None:
        output = gb.replace_origin_in_gb(
            "scotland", data_dir=data_dir, staging_dir=tmp_path / ".staging"
        )

        assert set(heating_costs(output)) == {
            "england-and-wales/domestic",
            "scotland/domestic",
        }


class TestRequireGbSegment:
    def test_domestic_has_a_gb_file(self) -> None:
        gb.require_gb_segment("domestic")

    @pytest.mark.parametrize("segment", ["non-domestic", "display"])
    def test_other_segments_raise_not_set_up(self, segment: str) -> None:
        with pytest.raises(gb.GbNotSetUpError, match="not set up yet"):
            gb.require_gb_segment(segment)
