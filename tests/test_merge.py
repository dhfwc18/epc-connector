"""
Tests for ``epc_connector.merge``.
"""

import datetime as dt
from pathlib import Path

import polars as pl
import pytest

from epc_connector import merge
from epc_connector.cleaning import Nation
from epc_connector.datasets import LAST_UPDATED_METADATA_KEY, RELEASE_METADATA_KEY
from epc_connector.mapping import MappingError, canonical_dtypes
from tests.fakes import write_raw_domestic

ENGLAND_AND_WALES_ROW = {
    "certificate_number": "0001-0002",
    "uprn": "100023336956",
    "local_authority": "W06000015",
    "lodgement_date": "2024-03-01",
    "lodgement_datetime": "2024-03-01 10:00:00",
    "construction_age_band": "England and Wales: 1930-1949",
    "tenure": "Rental (private)",
    "total_floor_area": "0",
    "heating_cost_current": "300",
    "walls_description": "Cavity wall, filled cavity",
    "walls_energy_eff": "Good",
    "mainheat_description": "Electric storage heaters",
}
SCOTLAND_ROW = {
    "REPORT_REFERENCE_NUMBER": "9001-0001",
    "OSG_REFERENCE_NUMBER": "151101923",
    "CREATED_AT": "08/12/2008 11:22:22",
    "HISTORIC": "True",
    "CONSTRUCTION_AGE_BAND": "2008 onwards",
    "HEATING_COST_CURRENT": "300",
    "WALL_DESCRIPTION": "Pitched, 250 mm loft insulation | Cavity wall",
}


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    data_dir = tmp_path / "data"
    write_raw_domestic(
        data_dir,
        Nation.ENGLAND_AND_WALES,
        [ENGLAND_AND_WALES_ROW],
        {LAST_UPDATED_METADATA_KEY: "2026-10-01"},
    )
    write_raw_domestic(
        data_dir, Nation.SCOTLAND, [SCOTLAND_ROW], {RELEASE_METADATA_KEY: "2026q2"}
    )
    return data_dir


def merge_into(data_dir: Path, tmp_path: Path) -> Path:
    return merge.merge_gb_domestic_epc(
        data_dir=data_dir, staging_dir=tmp_path / ".staging"
    )


class TestMergeGbDomesticEpc:
    def test_file_has_the_canonical_columns_and_types(
        self, data_dir: Path, tmp_path: Path
    ) -> None:
        output = merge_into(data_dir, tmp_path)

        assert output == data_dir / "domestic_epc_certificate_gb.parquet"
        assert dict(pl.read_parquet_schema(output)) == canonical_dtypes()

    def test_both_nations_are_mapped_and_stacked_in_order(
        self, data_dir: Path, tmp_path: Path
    ) -> None:
        wales, scotland = pl.read_parquet(merge_into(data_dir, tmp_path)).iter_rows(
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
        stored = pl.read_parquet_metadata(merge_into(data_dir, tmp_path))

        assert stored[LAST_UPDATED_METADATA_KEY] == "2026-10-01"
        assert stored[RELEASE_METADATA_KEY] == "2026q2"
        assert not (tmp_path / ".staging").exists()

    def test_unmapped_value_stops_before_writing(
        self, data_dir: Path, tmp_path: Path
    ) -> None:
        row = SCOTLAND_ROW | {"CONSTRUCTION_AGE_BAND": "1700s"}
        write_raw_domestic(data_dir, Nation.SCOTLAND, [row], {})

        with pytest.raises(MappingError, match="1700s"):
            merge_into(data_dir, tmp_path)

        assert not (data_dir / merge.GB_PARQUET_NAME).exists()
