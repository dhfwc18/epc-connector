"""
Tests for ``epc_connector.cleaning.domestic``.
"""

from pathlib import Path

import polars as pl
import pytest

from epc_connector.cleaning import domestic
from epc_connector.cleaning.descriptions import UnknownPhraseError
from epc_connector.cleaning.text import Nation
from tests.fakes import write_raw_domestic

EW = Nation.ENGLAND_AND_WALES


class TestCleanDomesticEpc:
    @pytest.fixture
    def data_dir(self, tmp_path: Path) -> Path:
        """A two-row England and Wales domestic file with real descriptions."""
        rows = [
            {
                "certificate_number": "c1",
                "walls_description": "Cavity wall, filled cavity",
                "roof_description": "Pitched, 250 mm loft insulation",
                "lighting_description": "Low energy lighting in 50% of fixed outlets",
                "mainheat_description": "Electric storage heaters",
                "mainheatcont_description": "Programmer, room thermostat and TRVs",
            },
            {"certificate_number": "c2", "walls_description": "SAP05:Walls"},
        ]
        write_raw_domestic(tmp_path / "data", EW, rows, {})
        return tmp_path / "data"

    def test_result_is_lazy_with_descriptions_replaced_in_place(
        self, data_dir: Path
    ) -> None:
        cleaned = domestic.clean_domestic_epc(EW, data_dir=data_dir)

        raw = pl.scan_parquet(data_dir / domestic.RAW_PARQUET_NAMES[EW])
        raw_columns = raw.collect_schema().names()
        columns = cleaned.collect_schema().names()
        descriptions = set(domestic.SOURCE_COLUMNS[EW].values())
        kept = [column for column in raw_columns if column not in descriptions]
        before_walls = raw_columns[raw_columns.index("walls_description") - 1]
        assert isinstance(cleaned, pl.LazyFrame)
        assert not descriptions & set(columns)
        assert [column for column in columns if column in raw_columns] == kept
        assert columns.index("walls_construction") == columns.index(before_walls) + 1

    def test_cleaned_values_are_joined_in_row_order(self, data_dir: Path) -> None:
        first, second = (
            domestic.clean_domestic_epc(EW, data_dir=data_dir)
            .collect()
            .iter_rows(named=True)
        )

        assert first["certificate_number"] == "c1"
        assert first["roof_loft_insulation_thickness"] == 250
        assert first["main_heating_fuel"] == "electric"
        assert first["heating_controls_trvs"] is True
        assert first["lighting_low_energy_percent"] == 50.0
        assert second["walls_discarded"] == "placeholder text"
        assert second["roof_type"] is None

    def test_uncovered_description_raises_before_anything_is_read(
        self, tmp_path: Path
    ) -> None:
        rows = [{"walls_description": "Straw bale"}]
        write_raw_domestic(tmp_path / "data", EW, rows, {})

        with pytest.raises(UnknownPhraseError, match="straw bale"):
            domestic.clean_domestic_epc(EW, data_dir=tmp_path / "data")
