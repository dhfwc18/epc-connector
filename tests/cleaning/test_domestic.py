"""
Tests for ``epc_connector.cleaning.domestic``.
"""

from pathlib import Path

import polars as pl
import pytest

from epc_connector.cleaning import domestic
from epc_connector.cleaning.text import Nation
from epc_connector.external.england_wales_epc import LAST_UPDATED_METADATA_KEY

EW = Nation.ENGLAND_AND_WALES


class TestCleanDomesticEpc:
    @pytest.fixture
    def raw_file(self, tmp_path: Path) -> Path:
        """A two-row England and Wales domestic file with real descriptions."""
        descriptions = {
            "walls_description": ["Cavity wall, filled cavity", "SAP05:Walls"],
            "roof_description": ["Pitched, 250 mm loft insulation", None],
            "floor_description": ["Solid, no insulation (assumed)", ""],
            "windows_description": ["Fully double glazed", "Single glazed"],
            "glazed_type": ["double glazing installed before 2002", ""],
            "lighting_description": ["Low energy lighting in 50% of fixed outlets", ""],
            "mainheat_description": ["Electric storage heaters", ""],
            "mainheatcont_description": ["Programmer, room thermostat and TRVs", ""],
            "hotwater_description": ["From main system", ""],
            "secondheat_description": ["None", ""],
            "main_fuel": ["mains gas (not community)", ""],
        }
        frame = pl.DataFrame(
            {"certificate_number": ["c1", "c2"], **descriptions, "uprn": ["1", "2"]}
        )
        path = tmp_path / "data" / domestic.RAW_PARQUET_NAMES[EW]
        path.parent.mkdir()
        frame.write_parquet(path, metadata={LAST_UPDATED_METADATA_KEY: "x"})
        return path

    def test_descriptions_are_replaced_by_cleaned_columns(
        self, raw_file: Path, tmp_path: Path
    ) -> None:
        output = domestic.clean_domestic_epc(
            EW, data_dir=raw_file.parent, staging_dir=tmp_path / ".staging"
        )

        frame = pl.read_parquet(output)
        assert output.name == "cleaned_domestic_epc_certificate_england_wales.parquet"
        assert frame.columns[:3] == [
            "certificate_number",
            "walls_construction",
            "walls_insulation",
        ]
        assert frame.columns[-1] == "uprn"
        assert "walls_description" not in frame.columns
        first, second = frame.iter_rows(named=True)
        assert first["roof_loft_insulation_thickness"] == 250
        assert first["main_heating_fuel"] == "electric"
        assert first["heating_controls_trvs"] is True
        assert first["lighting_low_energy_percent"] == 50.0
        assert second["walls_discarded"] == "placeholder text"
        assert second["roof_type"] is None

    def test_raw_metadata_is_copied_and_staging_removed(
        self, raw_file: Path, tmp_path: Path
    ) -> None:
        output = domestic.clean_domestic_epc(
            EW, data_dir=raw_file.parent, staging_dir=tmp_path / ".staging"
        )

        assert pl.read_parquet_metadata(output)[LAST_UPDATED_METADATA_KEY] == "x"
        assert not (tmp_path / ".staging").exists()
